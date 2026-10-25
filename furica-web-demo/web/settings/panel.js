import { json } from '../host.js';

// Presentation metadata for existing HostConfig fields. Defaults and validation stay in Host.
const modelProtocols = ['openai_compatible', 'openai_responses', 'anthropic'];
const providers = ['aliyun', 'volcengine'];
const groups = [
  { id: 'model', title: '文本模型', selector: ['model.protocol', '接口协议', modelProtocols], fields: [
    ['endpoint', '完整接口地址', 'text', true], ['model', '模型名称', 'text', true],
    ['isolation_scope', '账号 / 项目隔离标识', 'text', true], ['api_key', 'API Key', 'secret', true],
    ['connect_timeout_ms', '连接超时（毫秒）', 'number'],
    ['max_tokens_field', '输出上限字段', ['max_tokens', 'max_completion_tokens'], false, ['openai_compatible']],
    ['supports_reasoning_effort', '支持 reasoning_effort', 'checkbox', false, ['openai_compatible']],
    ['supports_reasoning', '支持 reasoning', 'checkbox', false, ['openai_responses']],
    ['version', '接口版本', 'text', false, ['anthropic']],
    ['supports_adaptive_thinking', '支持 adaptive thinking', 'checkbox', false, ['anthropic']],
    ['supports_thinking_budget', '支持 thinking budget', 'checkbox', false, ['anthropic']],
  ] },
  { id: 'speech.stt', title: '语音识别 STT', selector: ['speech.stt.provider', '服务商', ['sensevoice', ...providers]], fields: [
    ['model_path', 'SenseVoice 模型绝对路径', 'text', true, ['sensevoice']],
    ['tokens_path', 'SenseVoice 词表绝对路径', 'text', true, ['sensevoice']],
    ['endpoint', '完整接口地址', 'text', true, providers], ['model', '模型名称', 'text', true, providers],
    ['api_key', 'API Key', 'secret', true, providers],
    ['resource_id', '资源 ID', 'text', true, ['volcengine']],
  ] },
  { id: 'speech.tts', title: '语音合成 TTS', selector: ['speech.tts.provider', '服务商', providers], fields: [
    ['endpoint', '完整接口地址', 'text', true], ['model', '模型名称', 'text', true],
    ['voice', '音色', 'text', true], ['api_key', 'API Key', 'secret', true],
    ['resource_id', '资源 ID', 'text', true, ['volcengine']],
  ] },
  { id: 'acp', title: '执行 Agent（ACP）', fields: [
    ['command', '可执行程序绝对路径', 'text', true], ['agent_id', 'Agent 标识', 'text', true],
    ['args', '启动参数（每项为一个完整参数）', 'list'], ['auth_method', '认证方式', 'nullable'],
    ['handshake_timeout_ms', '握手超时（毫秒）', 'number'], ['run_timeout_ms', '执行超时（毫秒）', 'number'],
    ['cancel_timeout_ms', '取消超时（毫秒）', 'number'], ['idle_timeout_ms', '空闲超时（毫秒）', 'number'],
  ] },
];
const limitFields = [
  ['channel_capacity', '通道容量'], ['connect_timeout_ms', '连接超时（毫秒）'],
  ['idle_timeout_ms', '空闲超时（毫秒）'], ['inference_timeout_ms', '推理超时（毫秒）'],
  ['max_history_messages', '历史消息上限'], ['max_context_bytes', '模型上下文字节上限'],
  ['max_output_tokens', '输出 Token 上限'], ['max_utterance_ms', '单段语音上限（毫秒）'],
  ['max_generated_audio_ms', '生成音频上限（毫秒）'], ['max_acp_sessions', 'ACP 会话上限'],
  ['max_runs', '执行轮次上限'], ['question_timeout_ms', '问题超时（毫秒）'],
  ['heartbeat_timeout_ms', '语音心跳超时（毫秒）'], ['upload_retry_limit', '音频上传重试次数'],
  ['upload_retry_window_ms', '音频上传重试窗口（毫秒）'], ['context_max_bytes', '会话导入字节上限'],
];
function node(tag, text, className) {
  const result = document.createElement(tag);
  if (text !== undefined) result.textContent = text;
  if (className) result.className = className;
  return result;
}
function get(config, path) { return path.split('.').reduce((value, key) => value?.[key], config); }
function set(config, path, value) {
  const keys = path.split('.'); const leaf = keys.pop();
  let owner = config;
  for (const key of keys) owner = owner[key] ??= {};
  if (value === undefined) delete owner[leaf]; else owner[leaf] = value;
}

export class SettingsPanel {
  constructor(onSaved) {
    this.onSaved = onSaved;
    this.form = document.querySelector('#service-settings');
    this.form.noValidate = true;
    this.status = document.querySelector('#settings-feedback');
    this.save = document.querySelector('#save-settings');
    this.fields = new Map(); this.sections = new Map();
    const help = node('p', '逐项填写配置，字段下方显示对应的配置键。API Key 直接保存在本机 SQLite，页面不回显；留空保留、输入替换，勾选清除可删除。本地路径使用绝对路径。超时等可选参数留空时，由 Host 使用默认值。', 'settings-help');
    this.form.append(help);
    for (const group of groups) {
      const section = this.section(group.id, group.title, group.id === 'model');
      if (group.selector) this.field(section, ...group.selector);
      else {
        this.acpEnabled = node('input'); this.acpEnabled.type = 'checkbox'; this.acpEnabled.id = 'setting-acp-enabled';
        const label = node('label', undefined, 'capability-toggle'); label.append(this.acpEnabled, document.createTextNode('启用 ACP Agent')); section.append(label);
      }
      for (const [key, title, type, required, choices] of group.fields) {
        this.field(section, `${group.id}.${key}`, title, type, required, choices, group.id);
      }
    }
    const vad = this.section('vad', '语音检测 VAD');
    this.field(vad, 'speech.vad_model', 'Silero 模型绝对路径', 'nullable');
    vad.append(node('p', '留空关闭 VAD。持续语音需要会话模型、VAD 和 STT；TTS 可选。'));
    const general = this.section('general', '工作目录与访问');
    this.field(general, 'workspace', '工作目录绝对路径', 'text', true);
    this.field(general, 'http.listen', 'Host 监听地址', 'text', true);
    this.field(general, 'http.token', 'Host 访问令牌', 'secret', true);
    this.field(general, 'http.allowed_origins', '允许的浏览器来源', 'list');
    general.append(node('p', '修改访问令牌后，重启 Host 与 Web，Web 会重新读取 access-token 文件。修改监听地址时同步调整 Web 启动参数。通过本 Web 外壳连接无需额外添加允许来源。'));
    const limits = this.section('limits', '容量与超时');
    for (const [key, title] of limitFields) this.field(limits, `limits.${key}`, title, 'number');
    this.form.append(node('p', '系统提示词在 Host 数据目录的 system-prompt.toml 中编辑，重启 Host 后生效。', 'settings-help'));
    this.acpEnabled.onchange = () => this.visibility();
    for (const group of groups.filter(group => group.selector)) this.fields.get(group.selector[0]).input.onchange = () => {
      for (const field of this.fields.values()) if (field.group === group.id && field.type === 'secret') { field.input.value = ''; field.clear.checked = false; }
      this.visibility();
    };
    this.form.onsubmit = event => { event.preventDefault(); void this.submit(); };
    this.visibility();
  }
  section(id, title, open = false) {
    const section = node('details', undefined, 'settings-section service-section'); section.open = open;
    section.dataset.section = id; section.append(node('summary', title));
    this.sections.set(id, section); this.form.append(section); return section;
  }
  field(parent, path, title, type = 'text', required = false, choices, group) {
    const row = node('div', undefined, 'settings-field');
    const id = `setting-${path.replaceAll('.', '-')}`;
    const label = node('label', title, 'field-label'); label.htmlFor = id;
    const marker = node('span', '必填', 'required-marker'); label.append(marker); marker.hidden = !required;
    const key = node('code', path, 'setting-key');
    const input = node(type === 'list' ? 'div' : Array.isArray(type) ? 'select' : 'input'); input.id = id;
    if (Array.isArray(type)) {
      for (const value of ['', ...type]) { const option = node('option', value || (groups.some(g => g.selector?.[0] === path) ? '未启用' : '使用 Host 默认值')); option.value = value; input.append(option); }
    } else if (type !== 'list') {
      input.type = type === 'secret' ? 'password' : type === 'number' || type === 'checkbox' ? type : 'text';
      if (type === 'number') { input.min = '0'; input.step = '1'; input.max = String(Number.MAX_SAFE_INTEGER); }
      if (type !== 'checkbox') input.spellcheck = false;
    }
    input.name = path; input.setAttribute('aria-describedby', `${id}-key`); key.id = `${id}-key`;
    row.append(label, key, input); parent.append(row);
    const field = { path, title, type, required, choices, group, input, row, marker, active: true };
    this.fields.set(path, field);
    if (type === 'list') this.list(field, []);
    if (type === 'secret') {
      input.autocomplete = 'new-password'; input.maxLength = 8192;
      if (path === 'http.token') input.minLength = 32;
      field.status = node('p', '尚未配置'); field.status.id = `${id}-status`;
      input.setAttribute('aria-describedby', `${id}-key ${id}-status`);
      field.clear = node('input'); field.clear.type = 'checkbox'; field.clear.id = `clear-${path.replaceAll('.', '-')}`;
      field.clearLabel = node('label', undefined, 'clear-key'); field.clearLabel.append(field.clear, document.createTextNode('清除已保存的密钥'));
      field.clear.onchange = () => { if (field.clear.checked) input.value = ''; this.visibility(); };
      row.append(field.status, field.clearLabel);
    }
    return input;
  }
  list(field, values) {
    field.input.replaceChildren();
    field.items = node('div', undefined, 'setting-list-items');
    const add = node('button', '添加一项', 'quiet-button'); add.type = 'button';
    add.onclick = () => this.listItem(field, '');
    field.input.append(field.items, add);
    for (const value of values) this.listItem(field, value);
  }
  listItem(field, value) {
    const row = node('div', undefined, 'setting-list-item');
    const input = node(field.path === 'acp.args' ? 'textarea' : 'input');
    if (input.tagName === 'TEXTAREA') input.rows = 1; else input.type = 'text';
    input.value = value; input.setAttribute('aria-label', `${field.title} ${field.items.children.length + 1}`);
    const remove = node('button', '移除', 'quiet-button'); remove.type = 'button';
    remove.setAttribute('aria-label', `移除${field.title}项`); remove.onclick = () => row.remove();
    row.append(input, remove); field.items.append(row);
  }
  secretConfigured(field) {
    const keys = field.path.split('.'); const key = keys.pop(); const owner = keys.join('.');
    const group = groups.find(group => group.id === field.group);
    return Boolean(get(this.config, `${owner}.${key}_configured`)) && (!group?.selector || get(this.config, group.selector[0]) === this.fields.get(group.selector[0]).input.value);
  }
  visibility() {
    for (const field of this.fields.values()) {
      let active = true;
      if (field.group) {
        const group = groups.find(group => group.id === field.group);
        const choice = group.selector ? this.fields.get(group.selector[0]).input.value : this.acpEnabled.checked;
        active = Boolean(choice) && (!field.choices || field.choices.includes(choice));
      }
      const configured = field.type === 'secret' && this.secretConfigured(field);
      const clearing = field.type === 'secret' && field.clear.checked;
      const required = field.required && !configured && !clearing;
      field.active = active; field.row.hidden = !active; field.marker.hidden = !active || !required;
      if (field.type === 'secret') {
        field.clearLabel.hidden = !configured || field.path === 'http.token';
        field.clear.disabled = !active;
        field.status.textContent = configured ? '已配置 · 留空保留，输入新值替换' : '尚未配置';
      }
      if (field.type !== 'list') { field.input.disabled = !active || clearing; field.input.required = active && required; }
      else for (const control of field.input.querySelectorAll('input, textarea, button')) control.disabled = !active;
    }
  }
  message(text, error = false) { this.status.textContent = text; this.status.classList.toggle('error', error); }
  populate(config) {
    this.config = structuredClone(config);
    this.acpEnabled.checked = config.acp != null;
    for (const field of this.fields.values()) {
      const value = get(config, field.path);
      if (field.type === 'secret') { field.input.value = ''; field.clear.checked = false; }
      else if (field.type === 'list') this.list(field, value ?? []);
      else if (field.type === 'checkbox') field.input.checked = value ?? false;
      else field.input.value = value ?? '';
    }
    this.visibility();
  }
  collect() {
    const config = structuredClone(this.config);
    for (const group of groups) set(config, group.id, null);
    for (const field of this.fields.values()) {
      if (!field.active) continue;
      let value;
      if (field.type === 'secret') value = field.clear.checked ? null : field.input.value || undefined;
      else if (field.type === 'list') value = [...field.items.querySelectorAll('input, textarea')].map(input => input.value);
      else if (field.type === 'checkbox') value = field.input.checked;
      else if (field.type === 'number') {
        value = field.input.value === '' ? undefined : Number(field.input.value);
        if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) throw new Error(`${field.title}需要非负整数。`);
      } else if (field.type === 'nullable') value = field.input.value.trim() ? field.input.value : null;
      else value = field.input.value === '' && !field.required ? undefined : field.input.value;
      if (groups.some(group => group.selector?.[0] === field.path) && !value) continue;
      set(config, field.path, value);
    }
    if (!this.acpEnabled.checked) config.acp = null;
    if (!config.speech?.vad_model && !config.speech?.stt && !config.speech?.tts) config.speech = null;
    return config;
  }
  async open() {
    if (this.saving || this.loading) return;
    this.loading = true; this.save.disabled = true; this.form.inert = true; this.message('正在读取 Host 配置…');
    try {
      this.populate(await json('/configuration')); this.save.disabled = false;
      this.message('按配置项填写；保存后需要重启 Host。API Key 可直接填写，留空保留已保存的值。');
    } catch (error) { this.message(error.message, true); }
    finally { this.loading = false; this.form.inert = false; }
  }
  async submit() {
    if (this.saving || this.save.disabled) return;
    if (!this.form.checkValidity()) {
      const invalid = this.form.querySelector(':invalid'); if (invalid) { invalid.closest('details').open = true; invalid.reportValidity(); } return;
    }
    let config;
    try { config = this.collect(); } catch (error) { this.message(error.message, true); return; }
    this.saving = true; this.save.disabled = true; this.form.inert = true; this.message('正在保存…');
    try {
      const result = await json('/configuration', { method: 'PUT', body: config });
      this.onSaved(result);
      try { this.populate(await json('/configuration')); }
      catch {
        for (const field of this.fields.values()) if (field.type === 'secret') field.input.value = '';
        this.message('已保存，但状态读取失败，请重新打开设置。', true); return;
      }
      this.message(result.restart_required ? '已保存，需要重启 Host 后生效。' : 'Host 配置已保存。');
    } catch (error) { this.message(`${error.message} 输入已保留。`, true); }
    finally { this.saving = false; this.save.disabled = false; this.form.inert = false; }
  }
}
