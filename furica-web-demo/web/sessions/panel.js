const labels = { opening: '正在连接', ready: '已连接', closing: '正在结束', closed: '已结束', disconnected: '已断开', prepared: '等待保存', queued: '等待执行', running: '正在进行', completed: '已完成', failed: '未完成', cancelled: '已取消', interrupted: '已中断' };
export function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}
export function icon(name) {
  const node = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  node.classList.add('icon'); node.setAttribute('aria-hidden', 'true');
  const use = document.createElementNS(node.namespaceURI, 'use'); use.setAttribute('href', `#i-${name}`); node.append(use);
  return node;
}

export class SessionPanel {
  constructor(request, onError) { Object.assign(this, { request, onError }); this.drafts = new Map(); this.data = {}; this.selected = null; this.enabled = false; this.render(); }
  update(data, selected, enabled) { Object.assign(this, { data, selected, enabled }); this.render(); }
  button(label, callback, disabled = false) {
    const button = element('button', label); button.type = 'button'; button.disabled = !this.enabled || disabled;
    button.onclick = async () => { try { await callback(); } catch (error) { this.onError(error.message); } };
    return button;
  }
  input(key, placeholder, initial = '') {
    const field = element('textarea'); field.placeholder = placeholder; field.setAttribute('aria-label', placeholder);
    field.dataset.draft = key; field.value = this.drafts.get(key) ?? initial;
    field.oninput = () => this.drafts.set(key, field.value); field.disabled = !this.enabled;
    return field;
  }
  // Controls express an explicit user request through the main conversation.
  // Only Core's model tool registry can execute ACP commands.
  ask(tool, parameters) { return this.request(`请调用 ${tool} 工具，参数为 ${JSON.stringify(parameters)}。请按工具返回的实际状态回复。`); }
  render() {
    const container = document.querySelector('#task-list');
    const focused = document.activeElement?.dataset.draft;
    const selection = focused ? [document.activeElement.selectionStart, document.activeElement.selectionEnd] : null;
    const fragment = document.createDocumentFragment();
    const sessions = (this.data.acp_sessions ?? []).filter(s => s.session_id === this.selected);
    document.querySelector('#activity-dot').hidden = !(this.data.questions ?? []).some(q => q.session_id === this.selected && q.state === 'pending');
    if (!sessions.length) {
      const empty = element('div', undefined, 'task-empty'); empty.append(icon('spark'), element('h3', '想法会在这里，慢慢成形。'), element('p', '在对话中交给我一件事，\n就能在这里看到它的进展。')); fragment.append(empty);
    }
    for (const session of sessions) {
      const card = element('section', undefined, 'task-session');
      const title = element('div', undefined, 'task-title'); title.append(element('span', session.agent_id?.split(/[\\/]/).pop() || '工作伙伴'), element('span', labels[session.state] ?? session.state, 'status-label'));
      card.append(title);
      const runs = (this.data.runs ?? []).filter(r => r.acp_session_id === session.id).slice(-8);
      for (const run of runs) {
        const row = element('div', undefined, 'task-run');
        row.append(element('span', labels[run.state] ?? run.state, 'status-label'));
        const input = (this.data.messages ?? []).find(m => m.id === run.input_message_id);
        const blocks = input?.content?.Blocks;
        const prompt = blocks?.map(b => b.text ?? '').join('') || input?.content?.Model?.User;
        if (prompt) row.append(element('p', prompt));
        if (run.summary || run.error) row.append(element('pre', run.error || run.summary));
        const messages = (this.data.messages ?? []).filter(m => m.run_id === run.id && m.role === 'Assistant' && m.content?.Blocks).slice(-3);
        for (const message of messages) {
          const text = message.content.Blocks.filter(b => b.type === 'text').map(b => b.text ?? '').join('');
          if (text) row.append(element('div', text.slice(0, 8000), 'agent-message'));
        }
        const tools = (this.data.tool_calls ?? []).filter(t => t.run_id === run.id);
        for (const tool of tools.slice(-5)) row.append(element('p', `${tool.title || tool.name || '工具调用'} · ${labels[tool.state?.toLowerCase()] || tool.state}`, 'task-meta'));
        card.append(row);
      }
      if (['opening', 'ready'].includes(session.state)) {
        const prompt = this.input(`continue-${session.id}`, '接下来，还想做什么？');
        card.append(prompt);
        const controls = element('div', undefined, 'task-controls');
        controls.append(this.button('交给对话', async () => {
          if (!prompt.value.trim()) return;
          await this.ask('prepare_next_run', { acp_session_id: session.id, prompt: prompt.value.trim() });
          this.drafts.delete(`continue-${session.id}`); prompt.value = '';
        }), this.button('请求取消', () => this.ask('cancel_run', { acp_session_id: session.id }), !runs.some(r => ['prepared', 'queued', 'running'].includes(r.state))), this.button('请求结束', () => this.ask('close_acp_session', { acp_session_id: session.id })));
        card.append(controls);
      }
      for (const question of (this.data.questions ?? []).filter(q => q.acp_session_id === session.id && q.state === 'pending')) {
        const box = element('div', undefined, 'question'); const kind = question.kind;
        box.append(element('strong', kind.title || kind.message));
        if (kind.summary) box.append(element('p', kind.summary));
        const answer = (action, content) => this.ask('answer_question', { question_id: question.id, action, ...(content === undefined ? {} : { content }) });
        if (kind.kind === 'permission') {
          for (const option of kind.options) box.append(this.button(option.name, () => answer('submit', option.id)));
        } else {
          const details = element('details'); details.append(element('summary', '查看需要补充的信息'), element('pre', JSON.stringify(kind.schema, null, 2))); box.append(details);
          const input = this.input(`question-${question.id}`, '用 JSON 填写回答', '{}'); box.append(input, this.button('提交给对话', () => answer('submit', JSON.parse(input.value))), this.button('暂不回答', () => answer('decline')));
        }
        box.append(this.button('取消问题', () => answer('cancel'))); card.append(box);
      }
      fragment.append(card);
    }
    container.replaceChildren(fragment);
    if (focused) { const field = container.querySelector(`[data-draft="${CSS.escape(focused)}"]`); if (field) { field.focus({ preventScroll: true }); field.setSelectionRange(...selection); } }
  }
}
