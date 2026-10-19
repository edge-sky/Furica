export class Diagnostics {
  constructor() {
    this.entries = [];
    this.list = document.querySelector('#event-list');
    this.filter = document.querySelector('#event-filter');
    this.filter.onchange = () => this.render();
    document.querySelector('#clear-events').onclick = () => { this.entries = []; this.render(); };
  }
  add(event) {
    if (event.type === 'audio' || event.type === 'ack') return;
    this.entries.push({ time: new Date().toLocaleTimeString('zh-CN', { hour12: false }), event });
    if (this.entries.length > 500) this.entries.shift();
    this.render();
  }
  render() {
    document.querySelector('#event-count').textContent = this.entries.length;
    const fragment = document.createDocumentFragment();
    for (const { time, event } of this.entries) {
      const isError = event.type.includes('error') || event.kind === 'error';
      const filter = this.filter.value;
      if (filter === 'error' && !isError || filter === 'task' && !event.type.startsWith('task') || filter === 'state' && !['state', 'started', 'stopped', 'kernel_stopped', 'playback_state', 'speaker'].includes(event.type)) continue;
      const row = document.createElement('div'); row.className = `log-row${isError ? ' error' : ''}`;
      const timestamp = document.createElement('span'); timestamp.textContent = time;
      const kind = document.createElement('span'); kind.className = 'log-kind'; kind.textContent = event.type + (event.kind ? ` / ${event.kind}` : '');
      const details = document.createElement('details');
      const summary = document.createElement('summary'); summary.textContent = event.message || event.state || event.decision || (event.type === 'configuration' ? '已读取宿主配置状态' : '查看事件数据');
      const data = document.createElement('pre'); data.textContent = JSON.stringify(event, null, 2).slice(0, 12000);
      details.append(summary, data); row.append(timestamp, kind, details); fragment.append(row);
    }
    this.list.replaceChildren(fragment);
    this.list.scrollTop = this.list.scrollHeight;
  }
}
