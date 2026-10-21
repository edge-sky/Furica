const sections = [
  ['text_model', '文本模型', [
    ['protocol', '接口协议', ['openai-compatible', 'openai-responses', 'anthropic']],
    ['endpoint', '完整接口地址'],
    ['model', '模型名称'],
    ['isolation_scope', '账号 / 项目隔离标识'],
    ['max_tokens_field', '输出上限字段（Chat Completions）', ['max_tokens', 'max_completion_tokens']],
  ]],
  ['stt', '语音识别（默认本地）', [
    ['provider', '服务商', ['aliyun', 'volcengine']],
    ['endpoint', '完整 HTTP 接口地址'],
    ['model', '模型名称'],
    ['resource_id', '资源 ID（火山）'],
  ]],
  ['tts', '语音合成 TTS', [
    ['provider', '服务商', ['aliyun', 'volcengine']],
    ['endpoint', '完整接口地址（阿里 WSS / 火山 HTTPS）'],
    ['model', '模型名称'],
    ['voice', '音色'],
    ['resource_id', '资源 ID（火山）'],
  ]],
  ['acp', '执行 Agent（ACP）', [
    ['command', '可执行程序'],
    ['args', '启动参数（JSON 字符串数组）'],
    ['auth_method', '认证方式（可选）'],
    ['workspace', '工作目录（已存在的绝对路径）'],
  ]],
];

function node(tag, text, className) {
  const result = document.createElement(tag);
  if (text) result.textContent = text;
  if (className) result.className = className;
  return result;
}

export class SettingsPanel {
  constructor(onSaved) {
    this.onSaved = onSaved;
    this.form = document.querySelector('#service-settings');
    this.status = document.querySelector('#settings-feedback');
    this.save = document.querySelector('#save-settings');
    this.inputs = new Map();
    this.keys = new Map();
    this.labels = new Map();
    this.configuredKeys = {};
    for (const [name, title, fields] of sections) {
      const details = node('details', '', 'settings-section service-section');
      details.open = name === 'text_model';
      details.append(node('summary', title));
      for (const [field, label, control] of fields) this.field(details, `${name}.${field}`, label, control, field === 'args');
      if (name !== 'acp') {
        const input = this.field(details, `${name}.api_key`, 'API Key');
        input.type = 'password'; input.autocomplete = 'new-password'; input.spellcheck = false;
        const keyStatus = node('p', '尚未配置'); keyStatus.id = `${name}-key-status`;
        input.setAttribute('aria-describedby', keyStatus.id);
        const label = node('label', '', 'clear-key');
        const clear = node('input'); clear.type = 'checkbox'; clear.id = `clear-${name}-key`;
        label.append(clear, document.createTextNode('清除已保存的 API Key'));
        clear.onchange = () => { input.disabled = clear.checked; if (clear.checked) input.value = ''; this.requirements(); };
        details.append(keyStatus, label);
        this.keys.set(name, { input, clear, keyStatus });
      }
      if (name === 'stt') details.append(node('p', '默认使用本地 SenseVoice，无需填写服务地址和 API Key。仅选择云端服务时填写。'));
      this.form.append(details);
    }
    const local = node('details', '', 'settings-section service-section');
    local.append(node('summary', '本地语音与对话指令'));
    this.field(local, 'vad_model_path', 'VAD 模型路径覆盖（可选）');
    local.append(node('p', '留空使用本地 Silero VAD 模型。'));
    this.field(local, 'instructions', '系统指令（可选）', undefined, true);
    this.form.append(local);
    for (const path of ['stt.provider', 'tts.provider', 'text_model.protocol']) this.inputs.get(path).onchange = () => this.requirements();
    this.requirements();
    this.form.onsubmit = event => { event.preventDefault(); void this.submit(); };
  }

  field(parent, path, title, control, multiline = false) {
    const id = `setting-${path.replace('.', '-')}`;
    const label = node('label', title, 'field-label'); label.htmlFor = id;
    const input = node(Array.isArray(control) ? 'select' : multiline ? 'textarea' : 'input');
    input.id = id; input.name = path;
    if (Array.isArray(control)) {
      for (const value of ['', ...control]) { const option = node('option', value || (path === 'stt.provider' ? '本地 SenseVoice（无需配置）' : '请选择')); option.value = value; input.append(option); }
    } else if (!multiline) input.type = 'text';
    if (multiline) input.rows = path === 'instructions' ? 4 : 2;
    const row = node('div', '', 'settings-field'); row.append(label, input); parent.append(row);
    this.labels.set(path, label); this.inputs.set(path, input);
    return input;
  }

  requirements() {
    const required = new Set(['text_model.protocol', 'text_model.endpoint', 'text_model.model', 'text_model.isolation_scope', 'text_model.api_key', 'tts.provider', 'tts.endpoint', 'tts.model', 'tts.voice', 'tts.api_key', 'acp.command', 'acp.workspace']);
    const localStt = !this.inputs.get('stt.provider').value;
    if (!localStt) for (const field of ['endpoint', 'model', 'api_key']) required.add(`stt.${field}`);
    for (const name of ['stt', 'tts']) if (this.inputs.get(`${name}.provider`).value === 'volcengine') required.add(`${name}.resource_id`);
    for (const [path, input] of this.inputs) {
      const [section, field] = path.split('.');
      const isRequired = required.has(path);
      const label = this.labels.get(path);
      if (isRequired && !label.querySelector('.required-marker')) label.append(node('span', '必填', 'required-marker'));
      if (!isRequired) label.querySelector('.required-marker')?.remove();
      input.setAttribute('aria-required', String(isRequired && !(field === 'api_key' && this.configuredKeys[section] && !this.keys.get(section).clear.checked)));
      let hidden = section === 'stt' && field !== 'provider' && localStt;
      if (field === 'resource_id') hidden ||= this.inputs.get(`${section}.provider`).value !== 'volcengine';
      if (path === 'text_model.max_tokens_field') hidden = this.inputs.get('text_model.protocol').value !== 'openai-compatible';
      input.closest('.settings-field').hidden = hidden;
    }
    const sttKey = this.keys.get('stt');
    sttKey.keyStatus.hidden = localStt; sttKey.clear.closest('label').hidden = localStt;
  }

  message(text, error = false) { this.status.textContent = text; this.status.classList.toggle('error', error); }

  async request(options) {
    const response = await fetch('/api/settings', { cache: 'no-store', ...options });
    const value = await response.json();
    if (!response.ok) throw new Error(value.error || '设置操作失败，请重试。');
    return value;
  }

  async open() {
    if (this.saving || this.loading) return;
    this.loading = true;
    this.save.disabled = true; this.form.inert = true; this.message('正在读取设置…');
    try {
      const value = await this.request();
      this.populate(value.settings); this.onSaved(value.status);
      this.message('API Key 留空时保留，勾选后清除。'); this.save.disabled = false;
    } catch (error) { this.message(error.message, true); }
    finally { this.loading = false; this.form.inert = false; }
  }

  populate(settings) {
    for (const [path, input] of this.inputs) {
      const [section, field] = path.split('.');
      const value = field ? settings[section]?.[field] : settings[section];
      input.value = field === 'api_key' ? '' : field === 'args' ? (value?.length ? JSON.stringify(value) : '') : value ?? '';
    }
    this.configuredKeys = {};
    for (const [name, { input, clear, keyStatus }] of this.keys) {
      this.configuredKeys[name] = Boolean(settings[name]?.api_key_configured);
      clear.checked = false; input.disabled = false;
      keyStatus.textContent = settings[name]?.api_key_configured ? '已配置 · 密钥不会回显' : '尚未配置';
    }
    this.requirements();
  }

  collect() {
    const value = { stt: {}, tts: {}, text_model: {}, acp: {} };
    for (const [path, input] of this.inputs) {
      const [section, field] = path.split('.');
      if (!field) { value[section] = input.value; continue; }
      let text = input.value;
      if (field === 'args') {
        try { text = JSON.parse(text || '[]'); } catch { throw new Error('ACP 启动参数需要 JSON 字符串数组，例如 ["--stdio"]。'); }
        if (!Array.isArray(text) || text.some(arg => typeof arg !== 'string')) throw new Error('ACP 启动参数的每一项必须是字符串。');
      } else if (field === 'auth_method' && !text) text = null;
      else if (field === 'api_key' && this.keys.get(section).clear.checked) text = null;
      value[section][field] = text;
    }
    return value;
  }

  async submit() {
    if (this.saving || this.save.disabled) return;
    let settings;
    try { settings = this.collect(); } catch (error) { this.message(error.message, true); return; }
    this.saving = true; this.save.disabled = true; this.form.inert = true; this.message('正在保存…');
    try {
      const value = await this.request({ method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(settings) });
      this.populate(value.settings); this.onSaved(value.status);
      this.message('已保存，下次启动对话时生效。正在进行的对话继续使用原配置。');
    } catch (error) { this.message(error.message, true); }
    finally { this.saving = false; this.save.disabled = false; this.form.inert = false; }
  }
}
