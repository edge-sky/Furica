// The AudioContext resamples hardware input to 16 kHz before this processor.
class CaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.frame = new Int16Array(1600);
    this.offset = 0;
  }
  process(inputs) {
    const channels = inputs[0];
    if (!channels?.length) return true;
    for (let i = 0; i < channels[0].length; i++) {
      let sample = 0;
      for (const channel of channels) sample += channel[i] / channels.length;
      sample = Math.max(-1, Math.min(1, sample));
      this.frame[this.offset++] = Math.round(sample * (sample < 0 ? 32768 : 32767));
      if (this.offset === 1600) {
        this.port.postMessage(this.frame.buffer, [this.frame.buffer]);
        this.frame = new Int16Array(1600);
        this.offset = 0;
      }
    }
    return true;
  }
}
registerProcessor('furica-capture', CaptureProcessor);
