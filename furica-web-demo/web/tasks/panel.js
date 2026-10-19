const element = (tag, text, className) => {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
};
const labels = { opening: '连接中', ready: '就绪', closing: '关闭中', closed: '已关闭', disconnected: '已断开', queued: '排队中', running: '执行中', completed: '已完成', failed: '失败', cancelled: '已取消', interrupted: '已中断' };

export class TaskPanel {
  constructor(send, notify) {
    Object.assign(this, { send, notify });
    this.sessions = new Map(); this.runs = new Map(); this.questions = new Map(); this.progress = new Map(); this.drafts = new Map();
    this.container = document.querySelector('#task-list');
    this.enabled = false;
  }
  setEnabled(enabled) { this.enabled = enabled; this.render(); }
  reset() { this.sessions.clear(); this.runs.clear(); this.questions.clear(); this.progress.clear(); this.drafts.clear(); this.render(); }
  consume(event) {
    if (event.type === 'task_snapshot') {
      this.sessions = new Map(event.sessions.map(item => [item.id, item]));
      this.runs = new Map(event.runs.map(item => [item.id, item]));
      this.questions = new Map(event.questions.map(item => [item.id, item]));
    } else if (event.type === 'task') {
      if (event.kind === 'session') this.sessions.set(event.session.id, event.session);
      if (event.kind === 'run') this.runs.set(event.run.id, event.run);
      if (event.kind === 'question') this.questions.set(event.question.id, event.question);
      if (event.kind === 'question_resolved') {
        const question = this.questions.get(event.question_id);
        if (question) question.state = event.state;
      }
      if (event.kind === 'progress') {
        const text = JSON.stringify(event.update, null, 2).slice(0, 4000);
        this.progress.set(event.run_id, text);
        const view = document.getElementById(`progress-${event.run_id}`);
        if (view) view.textContent = text;
        return;
      }
    } else return;
    this.render();
  }
  button(text, action, disabled = false) {
    const button = element('button', text, 'button secondary small'); button.type = 'button'; button.disabled = !this.enabled || disabled;
    button.onclick = () => { try { action(); } catch (error) { this.notify(error.message); } };
    return button;
  }
  textarea(key, placeholder, initial = '') {
    const textarea = element('textarea'); textarea.value = this.drafts.get(key) ?? initial; textarea.placeholder = placeholder; textarea.setAttribute('aria-label', placeholder);
    textarea.disabled = !this.enabled; textarea.maxLength = 16000; textarea.oninput = () => this.drafts.set(key, textarea.value);
    return textarea;
  }
  render() {
    document.querySelector('#task-count').textContent = this.sessions.size;
    if (!this.sessions.size) {
      const empty = element('div', undefined, 'empty-state compact');
      empty.append(element('div', '▤', 'task-icon'), element('h3', '任务会在这里展开'), element('p', '通过对话让模型创建任务，在这里查看进度与处理问题。'));
      this.container.replaceChildren(empty); return;
    }
    const fragment = document.createDocumentFragment();
    for (const session of [...this.sessions.values()].sort((a, b) => b.created_at_ms - a.created_at_ms)) {
      const card = element('article', undefined, 'task-session');
      const head = element('div', undefined, 'task-title');
      const title = element('code', session.id.slice(0, 8)); title.title = session.id;
      head.append(title, element('span', labels[session.state] || session.state, 'badge'));
      card.append(head, element('div', session.workspace, 'task-meta'));
      if (session.error) card.append(element('div', session.error, 'task-meta'));
      const runs = [...this.runs.values()].filter(run => run.acp_session_id === session.id).sort((a, b) => a.created_at_ms - b.created_at_ms);
      for (const [index, run] of runs.entries()) {
        const row = element('div', undefined, 'task-run');
        row.append(element('div', `第 ${index + 1} 轮 · ${labels[run.state] || run.state}`), element('pre', run.prompt));
        if (run.summary || run.error) row.append(element('pre', run.error || run.summary));
        const progress = element('pre', this.progress.get(run.id) || ''); progress.id = `progress-${run.id}`; row.append(progress);
        card.append(row);
      }
      const terminal = ['closed', 'disconnected', 'closing'].includes(session.state);
      if (!terminal) {
        const prompt = this.textarea(`continue-${session.id}`, '继续此任务：输入下一轮要求'); card.append(prompt);
        const controls = element('div', undefined, 'task-controls');
        controls.append(this.button('继续执行', () => {
          if (!prompt.value.trim()) throw new Error('请填写下一轮任务要求。');
          this.send({ type: 'continue_task', session_id: session.id, prompt: prompt.value });
          this.drafts.delete(`continue-${session.id}`); prompt.value = '';
        }), this.button('取消当前轮次', () => this.send({ type: 'cancel_task', session_id: session.id }), !runs.some(run => ['running', 'queued'].includes(run.state))), this.button('关闭任务', () => this.send({ type: 'close_task', session_id: session.id })));
        card.append(controls);
      }
      for (const question of this.questions.values()) {
        if (question.acp_session_id !== session.id || question.state !== 'pending') continue;
        const box = element('div', undefined, 'question');
        const kind = question.kind;
        box.append(element('strong', kind.kind === 'permission' ? kind.title : kind.message));
        if (kind.summary) box.append(element('p', kind.summary));
        const answer = value => this.send({ type: 'answer_question', question_id: question.id, answer: value });
        if (kind.kind === 'permission') {
          for (const option of kind.options) box.append(this.button(option.name, () => answer({ action: 'submit', content: option.id })));
        } else {
          const schema = element('details'); schema.append(element('summary', '查看问题字段说明'), element('pre', JSON.stringify(kind.schema, null, 2))); box.append(schema);
          const input = this.textarea(`question-${question.id}`, 'JSON 回答，例如 {"name":"..."}', '{}'); box.append(input);
          box.append(this.button('提交回答', () => answer({ action: 'submit', content: JSON.parse(input.value) })), this.button('拒绝回答', () => answer({ action: 'decline' })));
        }
        box.append(this.button('取消问题', () => answer({ action: 'cancel' }))); card.append(box);
      }
      fragment.append(card);
    }
    this.container.replaceChildren(fragment);
  }
}
