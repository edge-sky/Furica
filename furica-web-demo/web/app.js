import { AudioBridge } from '/conversation/audio.js';
import { TaskPanel } from '/tasks/panel.js';
import { Diagnostics } from '/diagnostics.js';

const $ = selector => document.querySelector(selector);
const labels = { standby: '待机', verifying: '声纹校验中', connecting: '连接中', active: '对话中', muted: '已静音', stopped: '已停止' };
let socket, configuration, running = false, starting = false, state = 'stopped', pendingTranscript;
const diagnostics = new Diagnostics();
function notice(message) { $('#notice').textContent = message; $('#notice').hidden = !message; }
function send(command) {
  if (socket?.readyState !== WebSocket.OPEN) throw new Error('宿主连接已断开，请重新连接。');
  socket.send(JSON.stringify(command));
}
function action(command) { try { send(command); } catch (error) { notice(error.message); } }
const panel = new TaskPanel(send, notice);
const audio = new AudioBridge(command => { if (running && socket?.readyState === WebSocket.OPEN) send(command); }, packet => {
  if (!running || socket?.readyState !== WebSocket.OPEN) throw new Error('音频连接已断开。');
  if (socket.bufferedAmount > 128 * 1024) throw new Error('音频发送积压，已停止采集，请重新启动内核。');
  socket.send(packet);
}, error => { notice(error.message); diagnostics.add({ type: 'capture_error', message: error.message }); action({ type: 'stop' }); }, text => { $('#audio-status').textContent = text; });

function update() {
  const connected = socket?.readyState === WebSocket.OPEN;
  const voice = $('#mode').value === 'voice';
  const missing = configuration?.[voice ? 'voice_missing' : 'text_missing'] ?? ['等待宿主配置状态'];
  $('#start').disabled = !connected || running || starting || missing.length > 0;
  $('#start').textContent = starting ? '启动中…' : '启动内核 ↗';
  $('#stop').disabled = !connected || (!running && !starting);
  $('#mode').disabled = running || starting;
  $('#speaker').disabled = !voice;
  $('#mute').disabled = !running;
  $('#mute').textContent = state === 'muted' ? '恢复' : '静音';
  $('#end').disabled = !running;
  $('#wake').disabled = !running || voice || state !== 'standby';
  $('#interrupt').disabled = !running || state !== 'active';
  $('#refresh-tasks').disabled = !running;
  $('#text').disabled = $('#send').disabled = !running || state !== 'active';
  $('#state').textContent = running ? labels[state] || state : '未启动';
  $('#state').className = `badge${state === 'active' ? ' green' : ''}`;
  $('#config-state').textContent = missing.length ? '等待配置' : '配置已就绪';
  $('#config-state').className = `badge ${missing.length ? 'amber' : 'green'}`;
  $('#config-summary').textContent = missing.length ? `${missing.length} 项配置待填写 · 查看缺项` : '所需配置已填写，启动时将校验适配器与本地资源';
  $('#missing').replaceChildren(...missing.map(text => { const item = document.createElement('li'); item.textContent = text; return item; }));
}

function transcript(event) {
  $('#empty-chat')?.remove();
  let node;
  if (pendingTranscript && pendingTranscript.user === event.user) node = pendingTranscript.node;
  else {
    const message = document.createElement('div'); message.className = `message${event.user ? ' user' : ''}`;
    const label = document.createElement('div'); label.className = 'message-label'; label.textContent = event.user ? '你' : 'Furica';
    node = document.createElement('div'); node.className = 'bubble'; message.append(label, node); $('#messages').append(message);
    // Bound the browser view; the Core owns the actual history.
    if ($('#messages').children.length > 200) $('#messages').firstElementChild.remove();
  }
  node.textContent += event.text;
  pendingTranscript = event.final_chunk ? null : { user: event.user, node };
  $('#messages').scrollTop = $('#messages').scrollHeight;
}

async function consume(event) {
  diagnostics.add(event);
  switch (event.type) {
    case 'configuration': configuration = event; break;
    case 'started':
      running = true; starting = false; state = event.state; panel.setEnabled(true); notice('');
      if ($('#mode').value === 'voice') {
        try { await audio.startCapture(); } catch (error) { notice(`麦克风启动失败：${error.message}`); action({ type: 'stop' }); }
      }
      break;
    case 'state': state = event.state; audio.mute(state === 'muted'); break;
    case 'stopped': case 'kernel_stopped':
      running = starting = false; state = 'stopped'; pendingTranscript = null; audio.reset(); panel.setEnabled(false);
      if (event.type === 'kernel_stopped') notice(event.message);
      break;
    case 'transcript': transcript(event); break;
    case 'audio': audio.enqueue(event); break;
    case 'playback_state': case 'clear_playback': audio.setGeneration(event.generation); break;
    case 'task': case 'task_snapshot': panel.consume(event); break;
    case 'resync':
      notice(`事件接收落后，已重新同步状态和任务；${event.lost} 条事件未能展示。`);
      state = event.state; audio.setGeneration(event.playback.generation); if (event.tasks) panel.consume(event.tasks); break;
    case 'start_error': starting = false; notice(event.message); audio.reset(); break;
    case 'capture_error': audio.stopCapture(); notice(event.message); action({ type: 'stop' }); break;
    case 'error': notice(event.message); break;
  }
  update();
}

function connect() {
  if (socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(socket.readyState)) return;
  $('#reconnect').hidden = true; $('#connection').textContent = '连接中';
  socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`);
  let received = Promise.resolve();
  socket.onopen = () => { $('#connection').textContent = '宿主已连接'; $('#connection-dot').className = 'dot online'; update(); };
  socket.onmessage = event => {
    received = received.then(() => consume(JSON.parse(event.data))).catch(error => { notice(error.message); diagnostics.add({ type: 'error', message: error.message }); if (running) action({ type: 'stop' }); });
  };
  socket.onclose = () => {
    running = starting = false; configuration = null; audio.reset(); panel.setEnabled(false); update();
    $('#connection').textContent = '宿主已断开'; $('#connection-dot').className = 'dot offline'; $('#reconnect').hidden = false;
    notice('连接已断开。请检查宿主是否运行；同一宿主只允许一个页面连接，关闭其他页面后再重新连接。');
  };
  socket.onerror = () => notice('无法连接宿主。请确认本地服务正在运行，且没有其他页面占用连接。');
}

$('#mode').onchange = update;
$('#speaker').onchange = () => { if (running) action({ type: 'speaker', decision: $('#speaker').value }); };
$('#reconnect').onclick = connect;
$('#start').onclick = async () => {
  starting = true; update(); notice('');
  try {
    await audio.prepare(); audio.reset(); panel.reset();
    send({ type: 'start', mode: $('#mode').value, speaker: $('#speaker').value });
  } catch (error) { starting = false; notice(error.message); update(); }
};
$('#stop').onclick = () => { audio.reset(); action({ type: 'stop' }); };
$('#mute').onclick = () => action({ type: 'mute', muted: state !== 'muted' });
$('#end').onclick = () => { audio.discardCurrent(); pendingTranscript = null; action({ type: 'end' }); };
$('#wake').onclick = () => action({ type: 'wake' });
$('#interrupt').onclick = () => { audio.discardCurrent(); action({ type: 'interrupt' }); };
$('#refresh-tasks').onclick = () => action({ type: 'snapshot' });
$('#composer').onsubmit = async event => {
  event.preventDefault();
  const text = $('#text').value.trim(); if (!text || !running || state !== 'active') return;
  try { await audio.prepare(); send({ type: 'text', text }); $('#text').value = ''; } catch (error) { notice(error.message); }
};
$('#text').onkeydown = event => { if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); $('#composer').requestSubmit(); } };
window.addEventListener('pagehide', () => { audio.reset(); socket?.close(); });
update(); connect();
