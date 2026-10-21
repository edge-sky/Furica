import { AudioBridge } from '/conversation/audio.js';
import { SessionPanel, element, icon } from '/sessions/panel.js';
import { Diagnostics } from '/diagnostics.js';
import { SettingsPanel } from '/settings/panel.js';

const $ = selector => document.querySelector(selector);
const labels = { standby: '随时可以开始', verifying: '正在听你说', connecting: '正在为你连接', active: '我在，慢慢说', muted: '已暂停聆听', stopped: '轻触，说点什么' };
let socket, configuration, running = false, starting = false, stopping = false, state = 'stopped';
let selected = null, activeSession = null, activeMode = null, pendingText = null, nextStart = null, waiting = false;
let requestSequence = 0;
let data = { sessions: [], messages: [], acp_sessions: [], runs: [], questions: [], tool_calls: [] };
const diagnostics = new Diagnostics();
const serviceSettings = new SettingsPanel(status => { configuration = status; update(); });
function notice(message) { $('#notice-text').textContent = message; $('#notice').hidden = !message; }
function settings() { if ($('#settings-dialog').open) return; $('#settings-dialog').showModal(); $('#conversation-menu').hidden = true; void serviceSettings.open(); }
function send(command) {
  if (socket?.readyState !== WebSocket.OPEN) throw new Error('连接已断开，重新连接后再试试。');
  socket.send(JSON.stringify(command));
}
function action(command) { try { send(command); } catch (error) { notice(error.message); } }
const panel = new SessionPanel(text => submitText(text), notice);
const audio = new AudioBridge(command => { if (running && socket?.readyState === WebSocket.OPEN) send(command); }, packet => {
  if (!running || socket?.readyState !== WebSocket.OPEN) throw new Error('语音连接已断开。');
  if (socket.bufferedAmount > 128 * 1024) throw new Error('音频发送积压，请重新开始语音对话。');
  socket.send(packet);
}, error => { notice(error.message); diagnostics.add({ type: 'capture_error', message: error.message }); action({ type: 'stop' }); }, text => { if (running && activeMode === 'voice') $('#audio-status').textContent = text; });

function update() {
  const connected = socket?.readyState === WebSocket.OPEN;
  const missing = configuration?.[$('#mode').value === 'voice' ? 'voice_missing' : 'text_missing'] ?? ['等待连接状态'];
  $('#send').disabled = !connected || starting || stopping || waiting;
  $('#text').disabled = starting || stopping;
  $('#microphone').disabled = !connected || starting || stopping;
  $('#microphone').classList.toggle('connecting', starting);
  $('#microphone').setAttribute('aria-pressed', String(running && activeMode === 'voice' && state !== 'muted'));
  $('#microphone').setAttribute('aria-label', running && activeMode === 'voice' ? (state === 'muted' ? '恢复聆听' : '暂停聆听') : '开始语音对话');
  $('#mode').disabled = running || starting;
  $('#stop').disabled = !running && !starting;
  $('#end').disabled = !running;
  $('#interrupt').hidden = !running || !waiting;
  $('#thinking').hidden = !waiting;
  $('#voice-state').textContent = starting ? '正在为你连接…' : running ? labels[state] || state : '轻触，说点什么';
  if (!running) $('#audio-status').textContent = '也可以用文字，让想法开始';
  else if (activeMode === 'text') $('#audio-status').textContent = '文字对话中 · 想法同样可以被听见';
  $('#config-state').textContent = missing.length ? '还需设置' : '已就绪';
  $('#config-summary').textContent = missing.length ? `${missing.length} 项配置待填写` : '连接配置已准备好';
  $('#missing').replaceChildren(...missing.map(text => element('li', text)));
  $('#model-info').textContent = configuration?.model ? `当前模型 · ${configuration.model}` : '尚未连接模型服务';
  $('#connection-dot').className = connected ? 'online' : 'offline';
  $('#connection').textContent = connected ? '本地已连接' : '连接已断开';
  $('#new-chat').disabled = starting || stopping;
  panel.update(data, selected, running && selected === activeSession && state === 'active' && !waiting);
}

function renderHistory() {
  const sessions = [...data.sessions].sort((a, b) => b.updated_at_ms - a.updated_at_ms);
  $('#session-count').textContent = String(sessions.length).padStart(2, '0');
  const fragment = document.createDocumentFragment();
  if (!sessions.length) fragment.append(element('p', '还没有对话。\n一个想法，就是好的开始。', 'history-empty'));
  for (const session of sessions) {
    const button = element('button', undefined, `session-item${session.id === selected ? ' active' : ''}`); button.type = 'button';
    button.setAttribute('aria-current', session.id === selected ? 'page' : 'false');
    const text = element('span', session.name); text.append(element('small', new Date(session.updated_at_ms).toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' })));
    button.append(icon('message'), text); button.onclick = () => selectSession(session.id); fragment.append(button);
  }
  $('#session-list').replaceChildren(fragment);
  $('#conversation-name').textContent = data.sessions.find(s => s.id === selected)?.name ?? '新的对话';
}

function visibleMessage(message) {
  if (message.session_id !== selected || message.acp_session_id) return null;
  const model = message.content?.Model;
  if (typeof model?.User === 'string') return { user: true, text: model.User };
  if (model?.Assistant) {
    const content = model.Assistant.content;
    const text = content.map(c => c.Text ?? '').join('') || content.filter(c => c.ToolCall?.name === 'speak').map(c => c.ToolCall.arguments.text ?? '').join('\n');
    if (text) return { user: false, text };
  }
  return null;
}
function renderMessages() {
  const container = $('#messages'); const nearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 90;
  const fragment = document.createDocumentFragment(); let count = 0;
  for (const message of [...data.messages].sort((a, b) => a.sequence - b.sequence)) {
    const value = visibleMessage(message); if (!value) continue; count++;
    const row = element('article', undefined, `message${value.user ? ' user' : ''}${message.state === 'Interrupted' ? ' interrupted' : ''}`);
    row.append(element('div', value.user ? '你' : 'Furica', 'message-label'), element('div', value.text, 'bubble')); fragment.append(row);
  }
  container.replaceChildren(fragment); container.hidden = count === 0; $('#welcome').hidden = count > 0;
  if (nearBottom || waiting) container.scrollTop = container.scrollHeight;
}
function showComposer(show = true) {
  $('#composer-wrap').hidden = !show; $('#keyboard-toggle').setAttribute('aria-expanded', String(show));
  document.body.classList.toggle('composing', show);
  if (show) $('#text').focus();
}
function sidebar(open) {
  if (matchMedia('(max-width:760px)').matches) { document.body.classList.toggle('sidebar-open', open); $('#sidebar-backdrop').hidden = !open; }
  else document.body.classList.toggle('sidebar-collapsed', !open);
  $('#sidebar-toggle').setAttribute('aria-expanded', String(open));
  $('.sidebar').inert = !open;
}
function activity(open) {
  $('#activity-panel').hidden = !open; $('#activity-rail').hidden = open;
  document.body.classList.toggle('panel-open', open); $('#activity-toggle').setAttribute('aria-expanded', String(open));
  if (open) $('#close-activity').focus();
}
function hasUnfinishedWork() { return waiting || data.runs.some(r => ['prepared', 'queued', 'running'].includes(r.state)); }
function selectSession(id) {
  if (starting || stopping) return;
  if (running && id !== selected) {
    if (hasUnfinishedWork()) { notice('还有工作正在进行。完成或停止当前连接后，再切换对话。'); return; }
    stopping = true; action({ type: 'stop' });
  }
  selected = id; waiting = false; pendingText = null;
  renderHistory(); renderMessages(); update();
  if (matchMedia('(max-width:760px)').matches) { sidebar(false); $('#sidebar-toggle').focus(); }
}
async function begin(mode) {
  const missing = configuration?.[mode === 'voice' ? 'voice_missing' : 'text_missing'];
  if (!missing || missing.length) { notice('先完成连接设置，我们就可以开始了。'); settings(); $('#config-details').open = true; return false; }
  try {
    await audio.prepare(); audio.reset(); starting = true; activeMode = mode; notice(''); update();
    send({ type: 'start', mode, speaker: $('#speaker').value, session_id: selected }); return true;
  } catch (error) { starting = false; pendingText = null; notice(error.message); update(); return false; }
}
function flushText() {
  if (pendingText && running && state === 'active') {
    const text = pendingText; pendingText = null; sendText(text);
  }
}
function sendText(text) {
  requestSequence = data.messages.reduce((latest, message) => message.session_id === selected ? Math.max(latest, message.sequence) : latest, 0);
  send({ type: 'text', text }); waiting = true;
}
function restorePending() {
  if (pendingText && !$('#text').value) { $('#text').value = pendingText; showComposer(); }
  pendingText = null;
}
async function submitText(text) {
  if (!text.trim() || starting || stopping || waiting) return false;
  if (running && state === 'active') { await audio.prepare(); sendText(text); update(); return true; }
  if (running && activeMode === 'text' && state === 'standby') { pendingText = text; send({ type: 'wake' }); return true; }
  if (running) { notice('请先恢复语音聆听，或停止当前连接后发送文字。'); return false; }
  pendingText = text;
  if (!await begin('text')) { pendingText = null; return false; }
  return true;
}

async function consume(event) {
  if (event.type === 'ack') return;
  diagnostics.add(event);
  switch (event.type) {
    case 'configuration': configuration = event; break;
    case 'session_snapshot':
      data = event.data;
      if (waiting && data.messages.some(m => m.session_id === selected && !m.acp_session_id && m.sequence > requestSequence && m.state === 'Complete' && m.content?.Model?.Assistant && !m.content.Model.Assistant.content.some(c => c.ToolCall))) waiting = false;
      renderHistory(); renderMessages(); break;
    case 'started':
      running = true; starting = stopping = false; state = event.state; selected = activeSession = event.session_id;
      notice(''); renderHistory();
      if (activeMode === 'voice') { try { await audio.startCapture(); } catch (error) { notice(`麦克风暂时无法开启：${error.message}`); action({ type: 'stop' }); } }
      flushText(); break;
    case 'state': state = event.state; audio.mute(state === 'muted'); flushText(); if (['standby', 'muted', 'stopped'].includes(state)) waiting = false; break;
    case 'transcript': if (!event.user) waiting = false; break;
    case 'stopped': case 'kernel_stopped':
      running = starting = stopping = waiting = false; activeSession = null; state = 'stopped'; pendingText = null; audio.reset();
      if (event.type === 'kernel_stopped') notice(event.message);
      if (nextStart) { const mode = nextStart; nextStart = null; await begin(mode); }
      break;
    case 'audio': audio.enqueue(event); waiting = false; break;
    case 'playback_state': case 'clear_playback': if (event.generation !== undefined) audio.setGeneration(event.generation); break;
    case 'execution_changed': if (event.kind === 'error' && (!event.session_id || event.session_id === selected)) notice(event.message); break;
    case 'resync': state = event.state; audio.setGeneration(event.playback.generation); if (event.sessions) { data = event.sessions.data; renderHistory(); renderMessages(); } break;
    case 'start_error': starting = waiting = false; restorePending(); notice(event.message); audio.reset(); break;
    case 'capture_error': audio.stopCapture(); notice(event.message); action({ type: 'stop' }); break;
    case 'save_error': $('#save-status').textContent = '保存失败 · 正在重试'; notice(event.message); break;
    case 'save_recovered': $('#save-status').textContent = '对话只保存在这台设备'; notice('会话已重新保存，可以继续了。'); break;
    case 'error': waiting = false; restorePending(); notice(event.message); break;
  }
  update();
}
function connect() {
  if (socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(socket.readyState)) return;
  $('#reconnect').hidden = true;
  socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`);
  let received = Promise.resolve();
  socket.onopen = () => { notice(''); update(); };
  socket.onmessage = event => { received = received.then(() => consume(JSON.parse(event.data))).catch(error => { notice(error.message); diagnostics.add({ type: 'error', message: error.message }); if (running) action({ type: 'stop' }); }); };
  socket.onclose = () => { running = starting = stopping = waiting = false; configuration = null; restorePending(); nextStart = null; audio.reset(); update(); $('#reconnect').hidden = false; notice('本地连接已断开。确认服务仍在运行，关闭其他占用连接的页面后重新连接。'); };
  socket.onerror = () => notice('暂时无法连接本地服务，请检查服务是否运行。');
}

$('#settings').onclick = $('#show-settings').onclick = settings;
$('#close-settings').onclick = $('#settings-done').onclick = () => $('#settings-dialog').close();
$('#settings-dialog').onclick = event => { if (event.target === event.currentTarget) { const box = event.currentTarget.getBoundingClientRect(); if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) event.currentTarget.close(); } };
$('#mode').onchange = update;
$('#speaker').onchange = () => { if (running) action({ type: 'speaker', decision: $('#speaker').value }); };
$('#sidebar-toggle').onclick = () => sidebar(matchMedia('(max-width:760px)').matches ? !document.body.classList.contains('sidebar-open') : document.body.classList.contains('sidebar-collapsed'));
$('#sidebar-backdrop').onclick = () => sidebar(false);
$('#new-chat').onclick = () => { selectSession(null); if (selected === null) { showComposer(false); notice(''); } };
$('#keyboard-toggle').onclick = () => showComposer($('#composer-wrap').hidden);
$('#activity-toggle').onclick = () => activity($('#activity-panel').hidden);
$('#activity-rail').onclick = () => activity(true); $('#close-activity').onclick = () => { activity(false); $('#activity-toggle').focus(); };
$('#refresh-sessions').onclick = () => action({ type: 'snapshot' });
$('#more').onclick = () => { $('#conversation-menu').hidden = !$('#conversation-menu').hidden; $('#more').setAttribute('aria-expanded', String(!$('#conversation-menu').hidden)); };
document.addEventListener('click', event => { if (!event.target.closest('#more, #conversation-menu')) { $('#conversation-menu').hidden = true; $('#more').setAttribute('aria-expanded', 'false'); } });
$('#dismiss-notice').onclick = () => notice(''); $('#reconnect').onclick = connect;
$('#microphone').onclick = async () => {
  if (running && activeMode === 'voice') action({ type: 'mute', muted: state !== 'muted' });
  else if (running) { if (hasUnfinishedWork()) { notice('先等这次回应完成，再切换到语音。'); return; } nextStart = 'voice'; stopping = true; audio.reset(); action({ type: 'stop' }); update(); }
  else await begin($('#mode').value);
};
$('#stop').onclick = () => { nextStart = pendingText = null; stopping = true; audio.reset(); action({ type: 'stop' }); $('#conversation-menu').hidden = true; update(); };
$('#end').onclick = () => { audio.discardCurrent(); waiting = false; action({ type: 'end' }); $('#conversation-menu').hidden = true; };
$('#interrupt').onclick = () => { audio.discardCurrent(); waiting = false; action({ type: 'interrupt' }); update(); };
$('#composer').onsubmit = async event => { event.preventDefault(); const text = $('#text').value.trim(); try { if (await submitText(text)) { $('#text').value = ''; $('#text').style.height = ''; } } catch (error) { notice(error.message); } };
$('#text').oninput = () => { $('#text').style.height = 'auto'; $('#text').style.height = `${Math.min($('#text').scrollHeight, 120)}px`; };
$('#text').onkeydown = event => { if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); $('#composer').requestSubmit(); } };
document.addEventListener('keydown', event => { if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); $('#new-chat').click(); } if (event.key === 'Escape') { activity(false); if (matchMedia('(max-width:760px)').matches) sidebar(false); $('#conversation-menu').hidden = true; } });
window.addEventListener('pagehide', () => { audio.reset(); socket?.close(); });
function syncNavigation() {
  const mobile = matchMedia('(max-width:760px)').matches;
  const open = mobile ? document.body.classList.contains('sidebar-open') : !document.body.classList.contains('sidebar-collapsed');
  $('.sidebar').inert = !open;
  $('#sidebar-toggle').setAttribute('aria-expanded', String(open));
  $('#sidebar-backdrop').hidden = !mobile || !open;
}
matchMedia('(max-width:760px)').addEventListener('change', syncNavigation);
syncNavigation();
update(); connect();
