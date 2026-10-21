import { test } from 'node:test';
import assert from 'node:assert/strict';
import { AudioBridge } from './audio.js';

function bridge(t) {
  const reports = [];
  const audio = new AudioBridge(value => reports.push(value), () => {}, error => { throw error; }, () => {});
  t.after(() => { clearInterval(audio.timer); audio.reset(); });
  audio.context = {
    state: 'running', currentTime: 0, destination: {},
    createBuffer: (_channels, length, rate) => ({ duration: length / rate, getChannelData: () => new Float32Array(length) }),
    createBufferSource: () => ({ connect() {}, disconnect() {}, stop() {}, start() {} }),
  };
  return { audio, reports };
}

test('reports cumulative actual playback through the final millisecond', t => {
  const { audio, reports } = bridge(t);
  const chunk = { generation: '1', item_id: 'speech-request', content_index: 0, sample_rate: 16000, samples: new Array(8000).fill(0) };
  audio.enqueue(chunk);
  audio.context.currentTime = .275; audio.report();
  assert.equal(reports.at(-1).played_ms, 250);
  audio.context.currentTime = .525; audio.report();
  assert.equal(reports.at(-1).played_ms, 500);
  audio.enqueue(chunk);
  audio.context.currentTime = 1.05; audio.report();
  assert.equal(reports.at(-1).played_ms, 1000);
  assert.equal(reports.at(-1).item_id, 'speech-request');
  const count = reports.length; audio.report(); assert.equal(reports.length, count);
});

test('an interruption rejects stale chunks until a newer generation arrives', t => {
  const { audio } = bridge(t);
  const chunk = { generation: '7', item_id: 'old-speech', content_index: 0, sample_rate: 16000, samples: [0, 0] };
  audio.enqueue(chunk); audio.discardCurrent(); audio.enqueue(chunk);
  assert.equal(audio.sources.size, 0);
  audio.enqueue({ ...chunk, generation: '6' }); assert.equal(audio.sources.size, 0);
  audio.enqueue({ ...chunk, generation: '8', item_id: 'new-speech' });
  assert.equal(audio.sources.size, 1);
  assert.equal(audio.items.has('new-speech:0'), true);
});
