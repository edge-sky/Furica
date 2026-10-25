// The shell forwards these paths to the existing Host HTTP contract.
export async function request(path, { body, headers, ...options } = {}) {
  const response = await fetch(`/api${path}`, {
    cache: 'no-store', ...options,
    headers: { ...(body !== undefined && !(body instanceof ArrayBuffer) ? { 'Content-Type': 'application/json' } : {}), ...headers },
    body: body === undefined || body instanceof ArrayBuffer ? body : JSON.stringify(body),
  });
  if (!response.ok) {
    const value = await response.json().catch(() => ({}));
    const error = new Error(value.message || `Host 请求失败（${response.status}）`);
    error.status = response.status; error.code = value.code;
    throw error;
  }
  return response;
}
export async function json(path, options) { return (await request(path, options)).json(); }

export async function pageAll(path, signal) {
  const items = [];
  for (let offset = 0; ; ) {
    const page = await json(`${path}?offset=${offset}&limit=200`, { signal });
    items.push(...page.items);
    offset += page.items.length;
    if (offset >= page.total) return items;
    if (!page.items.length) throw new Error('Host 分页结果不完整，请重新刷新。');
  }
}

// HTTP chunks may split UTF-8 characters, lines and entire SSE messages.
export async function readEvents(body, receive) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let pending = '', data = [];
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) throw new Error('Host 事件连接已断开，请重新连接。');
      pending += decoder.decode(value, { stream: true });
      if (pending.length > 1024 * 1024) throw new Error('Host 事件超出大小限制。');
      let end;
      while ((end = pending.indexOf('\n')) !== -1) {
        const line = pending.slice(0, end).replace(/\r$/, ''); pending = pending.slice(end + 1);
        if (!line) {
          if (data.length) receive(JSON.parse(data.join('\n')));
          data = [];
        } else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
      }
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
