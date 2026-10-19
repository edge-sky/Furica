export class AudioBridge {
  constructor(send, sendAudio, onError, onStatus) {
    Object.assign(this, { send, sendAudio, onError, onStatus });
    this.sequence = 0n;
    this.generation = null;
    this.blockedGeneration = null;
    this.sources = new Set();
    this.items = new Map();
    this.tail = 0;
    this.timer = setInterval(() => this.report(), 100);
  }
  async prepare() {
    if (!this.context || this.context.state === 'closed') {
      this.context = new AudioContext({ sampleRate: 16000 });
      if (this.context.sampleRate !== 16000) throw new Error('当前浏览器无法建立 16 kHz 音频上下文。');
    }
    await this.context.resume();
    if (this.context.state !== 'running') throw new Error('请点击页面以允许音频播放。');
  }
  async startCapture() {
    await this.prepare();
    if (this.stream) return;
    if (!navigator.mediaDevices?.getUserMedia) throw new Error('浏览器没有提供麦克风接口；请使用支持麦克风的本地浏览器。');
    this.stream = await navigator.mediaDevices.getUserMedia({ audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true } });
    try {
      await this.context.audioWorklet.addModule('/conversation/capture-worklet.js');
      this.capture = new AudioWorkletNode(this.context, 'furica-capture');
      this.input = this.context.createMediaStreamSource(this.stream);
      this.silent = this.context.createGain();
      this.silent.gain.value = 0;
      this.capture.port.onmessage = event => {
        const samples = new Int16Array(event.data);
        const packet = new ArrayBuffer(8 + samples.length * 2);
        const view = new DataView(packet);
        view.setBigUint64(0, this.sequence++, true);
        samples.forEach((sample, i) => view.setInt16(8 + i * 2, sample, true));
        try { this.sendAudio(packet); } catch (error) { this.stopCapture(); this.onError(error); }
      };
      this.capture.onprocessorerror = () => { this.stopCapture(); this.onError(new Error('音频采集处理器退出，请重新启动内核。')); };
      this.input.connect(this.capture).connect(this.silent).connect(this.context.destination);
      for (const track of this.stream.getTracks()) track.onended = () => { this.stopCapture(); this.onError(new Error('麦克风已断开，请重新启动内核。')); };
      this.onStatus('麦克风已开启 · 16 kHz');
    } catch (error) { this.stopCapture(); throw error; }
  }
  mute(muted) {
    for (const track of this.stream?.getTracks() || []) track.enabled = !muted;
    this.onStatus(this.stream ? (muted ? '麦克风已静音' : '麦克风已开启 · 16 kHz') : '麦克风未开启');
  }
  stopCapture() {
    if (this.capture) { this.capture.port.onmessage = null; this.capture.disconnect(); }
    this.input?.disconnect();
    this.silent?.disconnect();
    for (const track of this.stream?.getTracks() || []) { track.onended = null; track.stop(); }
    this.capture = this.input = this.silent = this.stream = null;
    this.onStatus('麦克风未开启');
  }
  setGeneration(value) {
    const incoming = BigInt(value);
    if (this.generation !== null && incoming <= this.generation) return;
    this.clearPlayback();
    this.generation = incoming;
    this.blockedGeneration = null;
  }
  discardCurrent() {
    this.blockedGeneration = this.generation;
    this.clearPlayback();
  }
  clearPlayback() {
    for (const source of this.sources) { source.onended = null; try { source.stop(); } catch {} source.disconnect(); }
    this.sources.clear();
    this.items.clear();
    this.tail = 0;
  }
  enqueue(chunk) {
    this.setGeneration(chunk.generation);
    if (BigInt(chunk.generation) !== this.generation || this.blockedGeneration === this.generation) return;
    if (!this.context || this.context.state !== 'running') throw new Error('音频播放已暂停，请点击页面后重新启动内核。');
    if (chunk.sample_rate !== 16000) throw new Error('宿主返回了不匹配的播放采样率。');
    if (this.tail - this.context.currentTime > 61) throw new Error('播放队列超过上限，请重新启动内核。');
    const samples = chunk.samples;
    if (!samples.length) return;
    const key = `${chunk.item_id}:${chunk.content_index}`;
    let item = this.items.get(key);
    if (!item) {
      item = { generation: chunk.generation, item_id: chunk.item_id, content_index: chunk.content_index, segments: [], total: 0, reported: -1 };
      this.items.set(key, item);
    }
    const buffer = this.context.createBuffer(1, samples.length, chunk.sample_rate);
    const channel = buffer.getChannelData(0);
    for (let i = 0; i < samples.length; i++) channel[i] = samples[i] / 32768;
    const source = this.context.createBufferSource();
    source.buffer = buffer;
    source.connect(this.context.destination);
    const start = Math.max(this.context.currentTime + .025, this.tail);
    item.segments.push({ start, length: samples.length, offset: item.total });
    item.total += samples.length;
    this.tail = start + buffer.duration;
    this.sources.add(source);
    source.onended = () => { this.sources.delete(source); source.disconnect(); this.report(); };
    source.start(start);
  }
  report() {
    if (!this.context || this.context.state !== 'running') return;
    for (const [key, item] of this.items) {
      let played = 0;
      for (const segment of item.segments) {
        const elapsed = this.context.currentTime >= segment.start + segment.length / 16000
          ? segment.length
          : Math.max(0, Math.min(segment.length, Math.floor((this.context.currentTime - segment.start) * 16000)));
        if (elapsed) played = segment.offset + elapsed;
      }
      const played_ms = Math.floor(played / 16);
      if (played_ms > item.reported) {
        item.reported = played_ms;
        this.send({ type: 'playback', generation: item.generation, item_id: item.item_id, content_index: item.content_index, played_ms });
      }
      if (played >= item.total && this.context.currentTime > this.tail + 2) this.items.delete(key);
    }
  }
  reset() {
    this.stopCapture();
    this.clearPlayback();
    this.generation = this.blockedGeneration = null;
  }
}
