import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pageAll, readEvents, request } from './host.js';

function body(chunks) {
  return new ReadableStream({ start(controller) { for (const chunk of chunks) controller.enqueue(chunk); controller.close(); } });
}
test('SSE decodes split Unicode, CRLF, comments and consecutive events', async () => {
  const bytes = new TextEncoder().encode(': ping\r\ndata: {"kind":"transcript",\r\ndata: "text":"你好"}\r\n\r\ndata: {"kind":"resync"}\n\n');
  const chunks = Array.from(bytes, byte => Uint8Array.of(byte));
  const events = [];
  await assert.rejects(readEvents(body(chunks), event => events.push(event)), /连接已断开/);
  assert.deepEqual(events, [{ kind: 'transcript', text: '你好' }, { kind: 'resync' }]);
});
test('pagination reads past 200 rows and preserves Host records', async t => {
  const paths = [];
  t.mock.method(globalThis, 'fetch', async path => {
    paths.push(path);
    const offset = Number(new URL(path, 'http://localhost').searchParams.get('offset'));
    return Response.json({ items: Array.from({ length: offset ? 1 : 200 }, (_, i) => ({ id: offset + i })), total: 201 });
  });
  assert.equal((await pageAll('/sessions')).length, 201);
  assert.deepEqual(paths, ['/api/sessions?offset=0&limit=200', '/api/sessions?offset=200&limit=200']);
});
test('request preserves status for bounded audio retries and sends PCM without JSON', async t => {
  const packet = new ArrayBuffer(3200);
  t.mock.method(globalThis, 'fetch', async (_path, options) => {
    assert.equal(options.body, packet);
    assert.equal(options.headers['Content-Type'], 'application/octet-stream');
    return Response.json({ code: 'queue_full', message: 'busy' }, { status: 429 });
  });
  await assert.rejects(request('/voice/id/audio/0', { method: 'PUT', body: packet, headers: { 'Content-Type': 'application/octet-stream' } }), error => error.status === 429 && error.code === 'queue_full');
});
