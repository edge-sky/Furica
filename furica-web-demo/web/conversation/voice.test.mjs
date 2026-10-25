import { test } from 'node:test';
import assert from 'node:assert/strict';
import { VoiceChannel, readAudio } from './voice.js';

function audioPacket(overrides = {}) {
  const header = new TextEncoder().encode(JSON.stringify({ channel_id: 'channel', generation: 3, item_id: 'speech', content_index: 0, sample_rate: 16000, samples: 2, ...overrides }));
  const bytes = new Uint8Array(4 + header.length + 4);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, header.length, true); bytes.set(header, 4);
  view.setInt16(4 + header.length, -200, true); view.setInt16(6 + header.length, 1200, true);
  return bytes;
}
test('audio framing survives arbitrary HTTP boundaries and parses signed PCM', async () => {
  const bytes = audioPacket(); const received = [];
  const stream = new ReadableStream({ start(controller) { for (const byte of bytes) controller.enqueue(Uint8Array.of(byte)); controller.close(); } });
  await assert.rejects(readAudio(stream, 'channel', chunk => received.push(chunk)), /音频连接已结束/);
  assert.equal(received.length, 1);
  assert.deepEqual([...received[0].samples], [-200, 1200]);
  assert.equal(received[0].generation, 3);
});
test('audio rejects an old channel and oversized headers', async () => {
  for (const bytes of [audioPacket({ channel_id: 'stale' }), Uint8Array.from([1, 16, 0, 0])]) {
    const stream = new ReadableStream({ start(controller) { controller.enqueue(bytes); controller.close(); } });
    await assert.rejects(readAudio(stream, 'channel', () => assert.fail('invalid audio was accepted')), /音频.*无效/);
  }
});
function channel(api) {
  const errors = [];
  const voice = new VoiceChannel({ reset() {} }, error => errors.push(error), () => {}, api);
  voice.lease = { channel_id: 'channel', voice_token: 'secret' }; voice.cancel = new AbortController(); voice.sequence = 0;
  voice.limits = { upload_retry_limit: 1, upload_retry_window_ms: 2000 };
  return { voice, errors };
}
async function until(check) {
  const end = Date.now() + 2000;
  while (!check()) { if (Date.now() > end) assert.fail('timed out'); await new Promise(resolve => setTimeout(resolve, 5)); }
}
test('only one upload is in flight; 429 retries the identical sequence and PCM', async () => {
  const calls = []; let inFlight = 0; let maximum = 0;
  const { voice, errors } = channel({ json: async (path, options) => {
    calls.push({ path, body: options.body }); maximum = Math.max(maximum, ++inFlight);
    await new Promise(resolve => setTimeout(resolve, 5)); inFlight--;
    if (calls.length === 1) { const error = new Error('busy'); error.status = 429; throw error; }
    const sequence = Number(path.split('/').at(-1)); return { sequence, next_sequence: sequence + 1 };
  } });
  const first = new ArrayBuffer(3200), second = new ArrayBuffer(3200);
  voice.upload(first); voice.upload(second);
  await until(() => !voice.uploading);
  assert.equal(maximum, 1); assert.equal(voice.sequence, 2); assert.deepEqual(errors, []);
  assert.deepEqual(calls.map(call => call.path), ['/voice/channel/audio/0', '/voice/channel/audio/0', '/voice/channel/audio/1']);
  assert.equal(calls[0].body, calls[1].body); assert.equal(calls[2].body, second);
});
test('capture backlog closes the lease instead of accumulating audio indefinitely', async () => {
  let closed = 0;
  const { voice, errors } = channel({ json: async (path, options) => {
    if (options.method === 'DELETE') { closed++; return {}; }
    return new Promise((_resolve, reject) => options.signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError'))));
  } });
  for (let i = 0; i < 6; i++) voice.upload(new ArrayBuffer(3200));
  await until(() => !voice.uploading);
  assert.equal(voice.lease, null); assert.equal(closed, 1); assert.equal(errors.length, 1); assert.equal(voice.queue.length, 0);
});
test('playback reports remain serialized and coalesce only pending positions', async () => {
  const reports = []; let inFlight = 0; let maximum = 0;
  const { voice } = channel({ json: async (_path, options) => {
    maximum = Math.max(maximum, ++inFlight); reports.push(options.body.played_ms);
    await new Promise(resolve => setTimeout(resolve, 5)); inFlight--; return {};
  } });
  const report = { type: 'playback', generation: 3, item_id: 'speech', content_index: 0 };
  voice.report({ ...report, played_ms: 0 }); voice.report({ ...report, played_ms: 100 }); voice.report({ ...report, played_ms: 200 });
  await until(() => !voice.reporting);
  assert.deepEqual(reports, [0, 200]); assert.equal(maximum, 1);
});
test('a late upload acknowledgement cannot advance a replacement channel', async () => {
  let acknowledge;
  const { voice } = channel({ json: async () => new Promise(resolve => { acknowledge = resolve; }) });
  voice.upload(new ArrayBuffer(3200));
  await until(() => acknowledge);
  voice.lease = { channel_id: 'replacement', voice_token: 'new-secret' };
  voice.sequence = 0; voice.queue = [];
  acknowledge({ sequence: 0, next_sequence: 1 });
  await until(() => !voice.uploading);
  assert.equal(voice.sequence, 0);
  assert.equal(voice.lease.channel_id, 'replacement');
});
