import { AudioBridge } from '/conversation/audio.js';
import { VoiceChannel } from '/conversation/voice.js';
import { SessionPanel, element, icon } from '/sessions/panel.js';
import { Diagnostics } from '/diagnostics.js';
import { SettingsPanel } from '/settings/panel.js';
import { json, request, pageAll, readEvents } from '/host.js';

const $ = selector => document.querySelector(selector);
const diagnostics = new Diagnostics();
let connected = false, health = {}, selected = null, busy = false, waiting = false, muted = false;
let requestSequence = 0, events, refreshed = false, refreshing = false, refreshAgain = false;
let data = { sessions: [], messages: [], acp_sessions: [], runs: [], questions: [], tool_calls: [] };
function notice(message) { $('#notice-text').textContent = message; $('#notice').hidden = !message; }
function settings() {
  if ($('#settings-dialog').open) return;
  $('#settings-dialog').showModal(); $('#conversation-menu').hidden = true; void serviceSettings.open();
}
const serviceSettings = new SettingsPanel(() => notice('配置已保存，重启 Host 后生效。'));
const panel = new SessionPanel(text => submitText(text), notice);
const audio = new AudioBridge(report => voice.report(report), packet => voice.upload(packet), error => void voice.fail(error), text => { if (voice.lease) $('#audio-status').textContent = text; });
const voice = new VoiceChannel(audio, error => notice(error.message), () => { muted = false; waiting = false; update(); void refresh(); });

function update() {
  const activeVoice = Boolean(voice.lease);
  const writable = !selected || data.sessions.find(s => s.id === selected)?.state !== 'Archived';
  $('#send').disabled = !connected || !health.text_available || !writable || busy || waiting;
  $('#text').disabled = busy;
  $('#microphone').disabled = !connected || !writable || busy || (!activeVoice && !health.voice_available);
  $('#microphone').classList.toggle('connecting', busy);
  $('#microphone').setAttribute('aria-pressed', String(activeVoice && !muted));
  $('#microphone').setAttribute('aria-label', activeVoice ? (muted ? '恢复聆听' : '暂停聆听') : '开始语音对话');
  $('#stop').disabled = !activeVoice || busy; $('#end').disabled = !activeVoice || busy;
  $('#interrupt').hidden = !connected || (!waiting && !health.playback_active);
  $('#thinking').hidden = !waiting;
  $('#voice-state').textContent = busy ? '正在为你连接…' : activeVoice ? (muted ? '已暂停聆听' : '我在，慢慢说') : '轻触，说点什么';
  if (!activeVoice) $('#audio-status').textContent = health.text_available ? '也可以用文字，让想法开始' : 'Host 尚未启用会话模型';
  const missing = [];
  if (!health.text_available) missing.push('文字对话：在 Host 中配置 model');
  if (!health.voice_available) missing.push('持续语音：在 Host 中配置会话模型、VAD 与 STT');
  $('#config-state').textContent = health.ready ? 'Host 已就绪' : 'Host 未就绪';
  $('#config-summary').textContent = missing.length ? '查看未启用的能力' : '文字与语音能力已就绪';
  $('#missing').replaceChildren(...missing.map(text => element('li', text)));
  $('#model-info').textContent = `文字 ${health.text_available ? '可用' : '未启用'} · 语音 ${health.voice_available ? '可用' : '未启用'} · ACP ${health.acp_available ? '可用' : '未启用'}`;
  $('#connection-dot').className = connected ? 'online' : 'offline';
  $('#connection').textContent = connected ? 'Host 已连接' : '连接已断开';
  $('#new-chat').disabled = busy;
  $('#save-status').textContent = health.storage === 'failed' ? 'Host 存储失败 · 请检查' : '对话由 Host 保存在本机';
  panel.update(data, selected, connected && health.text_available && writable && !busy && !waiting);
}
function renderHistory() {
  const sessions = [...data.sessions].sort((a, b) => b.updated_at_ms - a.updated_at_ms);
  $('#session-count').textContent = String(sessions.length).padStart(2, '0');
  const fragment = document.createDocumentFragment();
  if (!sessions.length) fragment.append(element('p', '还没有对话。\n一个想法，就是好的开始。', 'history-empty'));
  for (const session of sessions) {
    const button = element('button', undefined, `session-item${session.id === selected ? ' active' : ''}`); button.type = 'button';
    button.setAttribute('aria-current', session.id === selected ? 'page' : 'false');
    const text = element('span', session.name); text.append(element('small', new Date(session.updated_at_ms).toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' }) + (session.state === 'Archived' ? ' · 已归档' : '')));
    button.append(icon('message'), text); button.onclick = () => selectSession(session.id); fragment.append(button);
  }
  $('#session-list').replaceChildren(fragment);
  $('#conversation-name').textContent = data.sessions.find(s => s.id === selected)?.name ?? '新的对话';
}

function visibleMessage(message) {
  if (message.session_id !== selected || message.acp_session_id) return null;
  const content = message.content;
  if (content?.kind === 'user') return { user: true, text: content.text };
  if (content?.kind === 'assistant') {
    const parts = content.parts;
    const text = parts.map(c => c.Text ?? '').join('') || parts.filter(c => c.ToolCall?.name === 'speak').map(c => c.ToolCall.arguments.text ?? '').join('\n');
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

async function refresh() {
  refreshAgain = true;
  if (refreshing) return;
  refreshing = true;
  try {
    while (refreshAgain) {
      refreshAgain = false;
      const target = selected, observedLease = voice.lease;
      const [current, sessions] = await Promise.all([json('/health'), pageAll('/sessions')]);
      health = current;
      if (observedLease && voice.lease === observedLease) {
        if (current.voice_channel_id !== voice.lease.channel_id) await voice.close();
        else audio.setGeneration(current.playback_generation);
      }
      data.sessions = sessions;
      if (!refreshed) { selected = current.active_session_id ?? null; refreshed = true; }
      if (selected !== target) { refreshAgain = true; continue; }
      if (target) {
        const names = ['messages', 'acp-sessions', 'runs', 'tool-calls', 'questions'];
        const pages = await Promise.all(names.map(name => pageAll(`/sessions/${target}/${name}`)));
        if (selected !== target) { refreshAgain = true; continue; }
        names.forEach((name, index) => { data[name.replaceAll('-', '_')] = pages[index]; });
      } else for (const name of ['messages', 'acp_sessions', 'runs', 'tool_calls', 'questions']) data[name] = [];
      if (waiting && data.messages.some(m => m.sequence > requestSequence && !m.acp_session_id && m.state === 'Complete' && m.content?.kind === 'assistant' && !m.content.parts.some(p => p.ToolCall))) waiting = false;
      renderHistory(); renderMessages(); update();
    }
  } catch (error) { notice(error.message); }
  finally { refreshing = false; }
}
function hasUnfinishedWork() { return data.runs.some(r => ['prepared', 'queued', 'running'].includes(r.state)) || data.questions.some(q => q.state === 'pending'); }
async function selectSession(id) {
  if (busy || id === selected) return;
  if (voice.lease || hasUnfinishedWork()) { notice('请先结束语音，并等待执行完成或处理待回答的问题，再切换会话。'); return; }
  busy = true; update();
  try {
    if (id && health.text_available && data.sessions.find(s => s.id === id)?.state !== 'Archived') await json('/interaction/session', { method: 'POST', body: { session_id: id } });
    selected = id; waiting = false; requestSequence = 0;
    data.messages = []; data.acp_sessions = []; data.runs = []; data.tool_calls = []; data.questions = [];
    renderHistory(); renderMessages();
    await refresh();
    if (matchMedia('(max-width:760px)').matches) { sidebar(false); $('#sidebar-toggle').focus(); }
  } catch (error) { notice(error.message); }
  finally { busy = false; update(); }
}
async function ensureSession() {
  if (selected && data.sessions.find(s => s.id === selected)?.state === 'Archived') throw new Error('这段对话已归档，可以查看历史或开启新对话。');
  if (!selected) {
    const session = await json('/sessions', { method: 'POST' });
    selected = session.id; data.sessions.push(session); renderHistory();
  }
  if (health.active_session_id !== selected) {
    await json('/interaction/session', { method: 'POST', body: { session_id: selected } });
    health.active_session_id = selected;
  }
  return selected;
}
async function submitText(text) {
  if (!text.trim() || busy || waiting) return false;
  if (!health.text_available) { notice('请先在 Host 中配置会话模型并重启。'); settings(); return false; }
  busy = true; update();
  try {
    const id = await ensureSession();
    requestSequence = data.messages.reduce((latest, m) => !m.acp_session_id ? Math.max(latest, m.sequence) : latest, 0);
    waiting = true;
    await json(`/sessions/${id}/text`, { method: 'POST', body: { text } });
    await refresh();
    return true;
  } catch (error) { waiting = false; notice(error.message); return false; }
  finally { busy = false; update(); }
}
function consume(event) {
  diagnostics.add({ ...event, type: event.kind });
  if (event.kind === 'playback' && voice.lease?.channel_id === event.channel_id) { audio.setGeneration(event.generation); health.playback_active = event.active; }
  if (event.kind === 'voice_closed' && voice.lease?.channel_id === event.channel_id) void voice.close();
  if (event.kind === 'storage') { health.storage = event.healthy ? 'healthy' : 'failed'; update(); return; }
  if (event.kind === 'error') { waiting = false; notice(event.message); }
  if (event.kind === 'transcript' && event.session_id === selected) waiting = event.user;
  if (event.kind === 'state' && event.session_id === selected && ['Standby', 'Stopped'].includes(event.state)) waiting = false;
  if (['sessions_changed', 'resync', 'state', 'transcript', 'voice_closed', 'error'].includes(event.kind)) void refresh();
  update();
}
async function connect() {
  events?.abort();
  const controller = new AbortController(); events = controller;
  $('#reconnect').hidden = true;
  try {
    const response = await request('/events', { signal: controller.signal });
    if (events !== controller) return;
    connected = true; notice(''); update();
    const stream = readEvents(response.body, consume);
    void refresh();
    await stream;
  } catch (error) {
    if (events !== controller || controller.signal.aborted) return;
    connected = false; waiting = false;
    await voice.close(); update(); $('#reconnect').hidden = false;
    notice(error.message);
  }
}
async function closeVoice() {
  if (busy) return;
  busy = true; update();
  try { await voice.close(); }
  finally { busy = false; update(); }
  $('#conversation-menu').hidden = true;
}

$('#settings').onclick = $('#show-settings').onclick = settings;
$('#close-settings').onclick = $('#settings-done').onclick = () => $('#settings-dialog').close();
$('#settings-dialog').onclick = event => { if (event.target === event.currentTarget) { const box = event.currentTarget.getBoundingClientRect(); if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) event.currentTarget.close(); } };
$('#sidebar-toggle').onclick = () => sidebar(matchMedia('(max-width:760px)').matches ? !document.body.classList.contains('sidebar-open') : document.body.classList.contains('sidebar-collapsed'));
$('#sidebar-backdrop').onclick = () => sidebar(false);
$('#new-chat').onclick = () => { void selectSession(null); };
$('#keyboard-toggle').onclick = () => showComposer($('#composer-wrap').hidden);
$('#activity-toggle').onclick = () => activity($('#activity-panel').hidden);
$('#activity-rail').onclick = () => activity(true); $('#close-activity').onclick = () => { activity(false); $('#activity-toggle').focus(); };
$('#refresh-sessions').onclick = () => void refresh();
$('#more').onclick = () => { $('#conversation-menu').hidden = !$('#conversation-menu').hidden; $('#more').setAttribute('aria-expanded', String(!$('#conversation-menu').hidden)); };
document.addEventListener('click', event => { if (!event.target.closest('#more, #conversation-menu')) { $('#conversation-menu').hidden = true; $('#more').setAttribute('aria-expanded', 'false'); } });
$('#dismiss-notice').onclick = () => notice(''); $('#reconnect').onclick = () => void connect();
$('#microphone').onclick = async () => {
  if (busy) return;
  if (voice.lease) { muted = !muted; audio.mute(muted); update(); return; }
  busy = true; update(); notice('');
  try { const id = await ensureSession(); await voice.open(id, health); muted = false; }
  catch (error) { notice(`语音暂时无法开启：${error.message}`); }
  finally { busy = false; update(); }
};
$('#stop').onclick = $('#end').onclick = () => void closeVoice();
$('#interrupt').onclick = async () => {
  try { audio.discardCurrent(); await json('/interaction/interrupt', { method: 'POST' }); waiting = false; update(); }
  catch (error) { notice(error.message); }
};
$('#composer').onsubmit = async event => { event.preventDefault(); const text = $('#text').value.trim(); if (await submitText(text)) { $('#text').value = ''; $('#text').style.height = ''; } };
$('#text').oninput = () => { $('#text').style.height = 'auto'; $('#text').style.height = `${Math.min($('#text').scrollHeight, 120)}px`; };
$('#text').onkeydown = event => { if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); $('#composer').requestSubmit(); } };
document.addEventListener('keydown', event => { if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); $('#new-chat').click(); } if (event.key === 'Escape') { activity(false); if (matchMedia('(max-width:760px)').matches) sidebar(false); $('#conversation-menu').hidden = true; } });
window.addEventListener('pagehide', () => { voice.release(); events?.abort(); });
function syncNavigation() {
  const mobile = matchMedia('(max-width:760px)').matches;
  const open = mobile ? document.body.classList.contains('sidebar-open') : !document.body.classList.contains('sidebar-collapsed');
  $('.sidebar').inert = !open; $('#sidebar-toggle').setAttribute('aria-expanded', String(open)); $('#sidebar-backdrop').hidden = !mobile || !open;
}
matchMedia('(max-width:760px)').addEventListener('change', syncNavigation);
syncNavigation(); update(); void connect();
