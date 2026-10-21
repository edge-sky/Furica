import base64
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(os.environ.get('FURICA_WEB_BIN', ROOT/'target/debug/furica-web-demo'))
if not BINARY.exists():
    BINARY = ROOT.parent/'target/debug/furica-web-demo'
CHROMIUM = os.environ.get('FURICA_CHROMIUM') or shutil.which('chromium')
state = {'models': 0, 'speech': []}

class Provider(BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/tts':
            state['speech'].append(request['req_params']['text'])
            body = (json.dumps({'code': 20000000, 'data': base64.b64encode(bytes(6400)).decode()})+'\n').encode()
            kind = 'application/json'
        elif self.path == '/chat':
            state['models'] += 1
            if request['messages'][-1]['role'] == 'tool':
                delta = {'role': 'assistant', 'content': '我在这里，陪你把想法理清楚。'}
                reason = 'stop'
            else:
                delta = {'role': 'assistant', 'tool_calls': [
                    {'index': i, 'id': f'speech_{state["models"]}_{i}', 'type': 'function', 'function': {'name': 'speak', 'arguments': json.dumps({'text': text}, ensure_ascii=False)}}
                    for i, text in enumerate(['你好，我们慢慢来。', '先说说你最想做的一件事。'])]}
                reason = 'tool_calls'
            values = [{'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}, {'choices': [{'index': 0, 'delta': {}, 'finish_reason': reason}]}]
            body = (''.join('data: '+json.dumps(v, ensure_ascii=False)+'\n\n' for v in values)+'data: [DONE]\n\n').encode()
            kind = 'text/event-stream'
        else:
            body = json.dumps({'output': {'text': '测试语音'}}).encode(); kind = 'application/json'
        self.send_response(200)
        self.send_header('Content-Type', kind); self.send_header('Content-Length', str(len(body)))
        self.end_headers(); self.wfile.write(body)

mock = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
threading.Thread(target=mock.serve_forever, daemon=True).start()
endpoint = f'http://127.0.0.1:{mock.server_port}'
with tempfile.TemporaryDirectory(prefix='furica-web-integration-') as temporary:
    root = Path(temporary)
    config = {
        'stt': {'provider': 'aliyun', 'endpoint': endpoint+'/stt', 'model': 'asr', 'api_key_env': 'FURICA_SMOKE_KEY'},
        'tts': {'provider': 'volcengine', 'endpoint': endpoint+'/tts', 'model': 'tts', 'api_key_env': 'FURICA_SMOKE_KEY', 'resource_id': 'fixture', 'voice': 'fixture'},
        'text_model': {'protocol': 'openai-compatible', 'endpoint': endpoint+'/chat', 'model': 'fixture', 'api_key_env': 'FURICA_SMOKE_KEY', 'isolation_scope': 'browser-integration'},
        'acp': {'command': str(Path(shutil.which('false') or '/bin/false')), 'workspace': str(ROOT.parent)},
    }
    config_path = root/'config.json'; config_path.write_text(json.dumps(config))
    env = os.environ.copy()
    env.update(FURICA_SMOKE_KEY='test-fixture', FURICA_DEMO_DATA_DIR=str(root/'data'))
    with socket.socket() as candidate:
        candidate.bind(('127.0.0.1', 0)); port = candidate.getsockname()[1]
    host_log = open(root/'host.log', 'w+')
    host = subprocess.Popen([str(BINARY), '--config', str(config_path), '--port', str(port)], env=env, stdout=host_log, stderr=host_log)
    url = f'http://127.0.0.1:{port}'
    try:
        for _ in range(100):
            try:
                with urlopen(url+'/health', timeout=.2): break
            except Exception: time.sleep(.05)
        with sync_playwright() as p:
            browser = p.chromium.launch(executable_path=CHROMIUM, headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            page = browser.new_page(viewport={'width': 1440, 'height': 900})
            errors = []; playback = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            def sent(frame):
                if isinstance(frame, str):
                    value = json.loads(frame)
                    if value.get('type') == 'playback': playback.append(value)
            page.on('websocket', lambda ws: ws.on('framesent', sent))
            page.goto(url)
            page.wait_for_function("() => document.querySelector('#connection').textContent === '本地已连接'")
            page.locator('#keyboard-toggle').click()
            page.locator('#text').fill('帮我把今天的想法理清楚')
            page.locator('#send').click()
            page.get_by_text('我在这里，陪你把想法理清楚。', exact=True).wait_for()
            page.wait_for_function("() => document.querySelectorAll('#session-list .session-item').length === 1")
            for _ in range(100):
                page.wait_for_timeout(50)
                finished = {f['item_id'] for f in playback if f['played_ms'] == 200}
                if len(finished) == 2 and len(state['speech']) == 2: break
            assert len(state['speech']) == 2, state
            assert len(finished) == 2, playback
            snapshot_path = root/'data'/'sessions.json'
            saved = json.loads(snapshot_path.read_text())
            assert len(saved['sessions']) == 1
            original_id = saved['sessions'][0]['id']
            assert any(m['content'].get('Model', {}).get('User') == '帮我把今天的想法理清楚' for m in saved['messages'])
            assert len([m for m in saved['messages'] if 'ToolResult' in m['content']]) == 2
            assert len([t for t in saved['tool_calls'] if t['name'] == 'speak' and t['output']['accepted']]) == 2
            page.locator('#more').click(); page.locator('#stop').click()
            page.wait_for_function("() => document.querySelector('#voice-state').textContent === '轻触，说点什么'")
            page.reload()
            page.wait_for_function("() => document.querySelector('#connection').textContent === '本地已连接'")
            page.locator('.session-item').click()
            page.get_by_text('我在这里，陪你把想法理清楚。', exact=True).wait_for()
            assert page.locator('#messages .message.user').count() == 1
            assert json.loads(snapshot_path.read_text())['sessions'][0]['id'] == original_id
            page.locator('#keyboard-toggle').click()
            page.locator('#text').fill('继续刚才的话题')
            page.locator('#send').click()
            page.wait_for_function("() => document.querySelectorAll('#messages .message.user').length === 2")
            for _ in range(100):
                page.wait_for_timeout(50)
                finished = {f['item_id'] for f in playback if f['played_ms'] == 200}
                if len(finished) == 4 and len(state['speech']) == 4: break
            assert len(finished) == 4 and len(state['speech']) == 4
            resumed = json.loads(snapshot_path.read_text())
            assert len(resumed['sessions']) == 1 and resumed['sessions'][0]['id'] == original_id
            assert len([m for m in resumed['messages'] if m['role'] == 'User' and not m['acp_session_id']]) == 2
            page.locator('#new-chat').click()
            assert page.locator('#welcome').is_visible()
            assert page.locator('.session-item').count() == 1
            assert not errors, errors
            browser.close()
        print('PASS: real WebSocket + Core + OpenAI-compatible adapter + two queued speak calls + actual browser playback reports + host persistence + reload/history/new conversation')
        print(f'Observed {state["models"]} model requests, 4 synthesis requests, 4 completed playback items across the same resumed business session')
    except Exception:
        host_log.flush(); host_log.seek(0); print(host_log.read())
        raise
    finally:
        host.send_signal(signal.SIGINT)
        try: host.wait(timeout=15)
        except subprocess.TimeoutExpired: host.kill(); host.wait()
        host_log.close(); mock.shutdown()
