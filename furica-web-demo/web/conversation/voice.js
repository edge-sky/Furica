import { json, request } from '../host.js';

export async function readAudio(body, channelId, receive) {
  const reader = body.getReader();
  let pending = new Uint8Array(0);
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) throw new Error('Host 音频连接已结束，请重新开启语音。');
      const bytes = new Uint8Array(pending.length + value.length);
      bytes.set(pending); bytes.set(value, pending.length); pending = bytes;
      let offset = 0;
      while (pending.length - offset >= 4) {
        const length = new DataView(pending.buffer, pending.byteOffset + offset, 4).getUint32(0, true);
        if (!length || length > 4096) throw new Error('Host 音频头无效。');
        if (pending.length - offset < 4 + length) break;
        const header = JSON.parse(new TextDecoder().decode(pending.subarray(offset + 4, offset + 4 + length)));
        if (header.channel_id !== channelId || header.sample_rate !== 16000 || !Number.isInteger(header.samples) || header.samples < 0 || header.samples > 16000) throw new Error('Host 音频格式无效。');
        const size = 4 + length + header.samples * 2;
        if (pending.length - offset < size) break;
        const pcm = new DataView(pending.buffer, pending.byteOffset + offset + 4 + length, header.samples * 2);
        const samples = new Int16Array(header.samples);
        for (let i = 0; i < samples.length; i++) samples[i] = pcm.getInt16(i * 2, true);
        receive({ ...header, samples });
        offset += size;
      }
      pending = pending.slice(offset);
      if (pending.length > 4 + 4096 + 32000) throw new Error('Host 音频消息超出限制。');
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}

export class VoiceChannel {
  constructor(audio, onError, onClosed, api = { json, request }) {
    Object.assign(this, { audio, onError, onClosed, api });
    this.lease = null; this.queue = []; this.reports = new Map();
  }
  async open(sessionId, limits) {
    if (this.lease) throw new Error('当前页面已有语音通道。');
    await this.audio.prepare();
    const lease = await this.api.json('/voice', { method: 'POST', body: { session_id: sessionId, owner: 'furica-web' } });
    this.lease = lease; this.limits = limits; this.sequence = lease.next_sequence;
    this.cancel = new AbortController(); this.queue = []; this.reports.clear();
    try {
      const response = await this.api.request(this.path('/audio'), this.options());
      void readAudio(response.body, lease.channel_id, chunk => {
        if (this.lease === lease) this.audio.enqueue(chunk);
      }).catch(error => this.fail(error, lease));
      // Audio downloading alone cannot detect an invalidated playback generation.
      const health = await this.api.json('/health', { signal: this.cancel.signal });
      this.audio.setGeneration(health.playback_generation);
      this.heartbeat = setInterval(() => {
        if (this.lease !== lease || this.beating === lease) return;
        this.beating = lease;
        void this.api.json(this.path('/heartbeat'), this.options({ method: 'POST' }))
          .catch(error => this.fail(error, lease)).finally(() => { if (this.beating === lease) this.beating = null; });
      }, lease.heartbeat_interval_ms);
      await this.audio.startCapture();
      if (this.lease !== lease) { this.audio.reset(); throw new Error('语音通道已结束。'); }
    } catch (error) { await this.close(); throw error; }
  }
  path(suffix) { return `/voice/${this.lease.channel_id}${suffix}`; }
  options(options = {}) {
    return { signal: this.cancel.signal, ...options, headers: { 'X-Voice-Token': this.lease.voice_token, ...options.headers } };
  }
  async fail(error, lease = this.lease) {
    if (!lease || this.lease !== lease || error.name === 'AbortError') return;
    this.onError(error);
    await this.close();
  }
  upload(packet) {
    if (!this.lease) return;
    // At most 500 ms locally; a stalled upload must not grow an unbounded capture queue.
    if (this.queue.length >= 5) { void this.fail(new Error('语音上传积压，请重新开启语音。')); return; }
    this.queue.push(packet);
    if (this.uploading !== this.lease) void this.drain(this.lease);
  }
  async drain(lease) {
    this.uploading = lease;
    try {
      while (this.lease === lease && this.queue.length) {
        const packet = this.queue[0];
        const started = Date.now();
        let retries = 0;
        while (true) {
          if (this.lease !== lease) return;
          try {
            const ack = await this.api.json(this.path(`/audio/${this.sequence}`), this.options({
              method: 'PUT', body: packet, headers: { 'Content-Type': 'application/octet-stream' },
              signal: AbortSignal.any([this.cancel.signal, AbortSignal.timeout(Math.max(1, this.limits.upload_retry_window_ms - (Date.now() - started)))]),
            }));
            if (this.lease !== lease) return;
            if (ack.sequence !== this.sequence || ack.next_sequence !== this.sequence + 1) throw new Error('Host 音频确认序号无效。');
            this.sequence = ack.next_sequence;
            break;
          } catch (error) {
            const retryable = error.status === 429 || error.name === 'TypeError' || error.name === 'TimeoutError' || error.status === 502;
            if (!retryable || retries >= this.limits.upload_retry_limit || Date.now() - started + 100 >= this.limits.upload_retry_window_ms) throw error;
            retries++;
            await new Promise(resolve => setTimeout(resolve, 100));
          }
        }
        this.queue.shift();
      }
    } catch (error) { await this.fail(error, lease); }
    finally { if (this.uploading === lease) this.uploading = null; }
  }
  report({ type: _type, ...report }) {
    if (!this.lease) return;
    const key = `${report.generation}:${report.item_id}:${report.content_index}`;
    this.reports.set(key, report);
    if (this.reporting !== this.lease) void this.flushReports(this.lease);
  }
  async flushReports(lease) {
    this.reporting = lease;
    try {
      while (this.lease === lease && this.reports.size) {
        const [key, report] = this.reports.entries().next().value;
        this.reports.delete(key);
        await this.api.json(this.path('/playback'), this.options({ method: 'POST', body: report }));
      }
    } catch (error) { await this.fail(error, lease); }
    finally { if (this.reporting === lease) this.reporting = null; }
  }
  async close() {
    const lease = this.lease;
    if (!lease) return;
    this.lease = null;
    clearInterval(this.heartbeat); this.cancel.abort();
    this.queue = []; this.reports.clear(); this.audio.reset();
    try {
      await this.api.json(`/voice/${lease.channel_id}`, {
        method: 'DELETE', headers: { 'X-Voice-Token': lease.voice_token }, signal: AbortSignal.timeout(5000),
      });
    } catch (error) { if (error.status !== 410) this.onError(error); }
    finally { this.onClosed(); }
  }
  // Page unload is best effort. Host's independent heartbeat timeout releases a lost owner.
  release() {
    const lease = this.lease;
    if (!lease) return;
    clearInterval(this.heartbeat); this.cancel.abort(); this.audio.reset(); this.lease = null;
    void this.api.request(`/voice/${lease.channel_id}`, { method: 'DELETE', keepalive: true, headers: { 'X-Voice-Token': lease.voice_token } }).catch(() => {});
  }
}
