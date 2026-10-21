import base64
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import sqlite3
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
state = {'models': 0, 'speech': [], 'model_paths': [], 'instructions': []}
THOUGHT = 'FIXTURE_REASONING_ONLY'

class Provider(BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/tts':
            state['speech'].append(request['req_params']['text'])
            body = (json.dumps({'code': 20000000, 'data': base64.b64encode(bytes(6400)).decode()})+'\n').encode()
            kind = 'application/json'
        elif self.path in ['/chat', '/chat-new']:
            state['model_paths'].append(self.path)
            state['instructions'].append(request['messages'][0]['content'])
            for message in request['messages']:
                if message['role'] == 'assistant': assert message['reasoning_content'] == THOUGHT
            state['models'] += 1
            if request['messages'][-1]['role'] == 'tool':
                delta = {'role': 'assistant', 'content': '我在这里，陪你把想法理清楚。'}
                reason = 'stop'
            else:
                delta = {'role': 'assistant', 'tool_calls': [
                    {'index': i, 'id': f'speech_{state["models"]}_{i}', 'type': 'function', 'function': {'name': 'speak', 'arguments': json.dumps({'text': text}, ensure_ascii=False)}}
                    for i, text in enumerate(['你好，我们慢慢来。', '先说说你最想做的一件事。'])]}
                reason = 'tool_calls'
            values = [{'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': '', 'reasoning_content': ''}, 'finish_reason': None}]}, {'choices': [{'index': 0, 'delta': {'reasoning_content': THOUGHT}, 'finish_reason': None}]}, {'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}, {'choices': [{'index': 0, 'delta': {}, 'finish_reason': reason}]}]
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
        'tts': {'provider': 'volcengine', 'endpoint': endpoint+'/tts', 'model': 'tts', 'api_key': 'test-fixture', 'resource_id': 'fixture', 'voice': 'fixture'},
        'text_model': {'protocol': 'openai-compatible', 'endpoint': endpoint+'/chat', 'model': 'fixture', 'api_key': 'test-fixture', 'isolation_scope': 'browser-integration'},
        'acp': {'command': str(Path(shutil.which('false') or '/bin/false')), 'workspace': str(ROOT.parent)},
    }
    # A legacy JSON file is deliberately ignored; settings start empty.
    (root/'config.json').write_text(json.dumps(config))
    env = os.environ.copy()
    env.update(FURICA_DEMO_DATA_DIR=str(root/'data'))
    with socket.socket() as candidate:
        candidate.bind(('127.0.0.1', 0)); port = candidate.getsockname()[1]
    host_log = open(root/'host.log', 'w+')
    host = subprocess.Popen([str(BINARY), '--port', str(port)], env=env, stdout=host_log, stderr=host_log)
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
            initial = page.request.get(url+'/api/settings').json()
            assert initial['settings']['text_model']['model'] == ''
            assert initial['status']['text_missing']
            assert not any(field.startswith('stt.') for field in initial['status']['voice_missing'])
            assert not any('VAD' in field or '本地 ASR' in field for field in initial['status']['voice_missing'])
            # Reject cross-origin writes, malformed/oversized bodies, and wrong methods.
            for headers, data, expected in [
                ({'Content-Type': 'application/json'}, config, 403),
                ({'Content-Type': 'application/json', 'Origin': 'https://other.example'}, config, 403),
                ({'Content-Type': 'text/plain', 'Origin': url}, '{}', 415),
                ({'Content-Type': 'application/json', 'Origin': url}, '{', 400),
                ({'Content-Type': 'application/json', 'Origin': url}, 'x'*65537, 413),
            ]:
                assert page.request.put(url+'/api/settings', headers=headers, data=data).status == expected
            assert page.request.post(url+'/api/settings').status == 405
            assert page.request.get(url+'/api/settings', headers={'Host':'other.example'}).status == 403
            page.locator('#settings').click()
            page.locator('#save-settings').wait_for(state='visible')
            page.wait_for_function("() => !document.querySelector('#save-settings').disabled")
            page.locator('#service-settings details').evaluate_all('(nodes) => nodes.forEach(n => n.open = true)')
            assert page.locator('#service-settings input[placeholder], #service-settings textarea[placeholder]').count() == 0
            assert page.locator('#setting-acp-args').input_value() == ''
            assert page.locator('#setting-vad_model_path').input_value() == ''
            for field in ['text_model-protocol', 'text_model-endpoint', 'text_model-model', 'text_model-isolation_scope', 'text_model-api_key', 'tts-provider', 'tts-endpoint', 'tts-model', 'tts-voice', 'tts-api_key', 'acp-command', 'acp-workspace']:
                assert page.locator(f'label[for="setting-{field}"] .required-marker').inner_text() == '必填'
            assert page.locator('label[for="setting-vad_model_path"] .required-marker').count() == 0
            assert not page.locator('#setting-stt-endpoint').is_visible()
            page.locator('#setting-stt-provider').select_option('aliyun')
            assert page.locator('#setting-stt-endpoint').is_visible()
            assert page.locator('label[for="setting-stt-api_key"] .required-marker').inner_text() == '必填'
            page.locator('#setting-stt-provider').select_option('')
            assert not page.locator('#setting-stt-endpoint').is_visible()
            assert page.locator('label[for="setting-stt-api_key"] .required-marker').count() == 0
            page.locator('#setting-tts-provider').select_option('volcengine')
            assert page.locator('label[for="setting-tts-resource_id"] .required-marker').inner_text() == '必填'
            page.locator('#setting-tts-provider').select_option('aliyun')
            assert not page.locator('#setting-tts-resource_id').is_visible()
            assert page.locator('label[for="setting-tts-resource_id"] .required-marker').count() == 0
            for section, values in config.items():
                for key, value in values.items():
                    control = page.locator(f'#setting-{section}-{key}')
                    if key in ['provider', 'protocol']: control.select_option(value)
                    else: control.fill(value)
            page.locator('#save-settings').click()
            page.get_by_text('已保存，下次启动对话时生效。正在进行的对话继续使用原配置。', exact=True).wait_for()
            assert page.locator('#setting-text_model-api_key').input_value() == ''
            public = page.request.get(url+'/api/settings').json()
            assert 'test-fixture' not in json.dumps(public)
            assert public['status']['text_missing'] == []
            assert public['settings']['text_model']['api_key_configured']
            # Invalid input stays in the form and does not replace the saved model URL.
            page.locator('#setting-text_model-endpoint').fill('http://unsafe.example/v1')
            page.locator('#save-settings').click()
            page.wait_for_function("() => document.querySelector('#settings-feedback').classList.contains('error')")
            assert page.locator('#setting-text_model-endpoint').input_value() == 'http://unsafe.example/v1'
            assert page.request.get(url+'/api/settings').json()['settings']['text_model']['endpoint'] == endpoint+'/chat'
            page.locator('#setting-text_model-endpoint').fill(endpoint+'/chat')
            page.locator('#clear-text_model-key').check()
            page.locator('#save-settings').click()
            page.wait_for_function("() => document.querySelector('#text_model-key-status').textContent === '尚未配置'")
            page.locator('#setting-text_model-api_key').fill('test-fixture')
            page.locator('#save-settings').click()
            page.wait_for_function("() => document.querySelector('#text_model-key-status').textContent.includes('已配置')")
            page.locator('#service-settings details').evaluate_all('(nodes) => nodes.forEach(n => n.open = false)')
            page.locator('#service-settings details').first.evaluate('(n) => n.open = true')
            page.locator('.settings-body').evaluate('(n) => n.scrollTop = 0')
            page.screenshot(path='/private/tmp/furica-settings-desktop.png', full_page=True)
            page.set_viewport_size({'width': 390, 'height': 844})
            page.wait_for_function('() => document.documentElement.scrollWidth <= innerWidth', timeout=3000)
            page.screenshot(path='/private/tmp/furica-settings-mobile.png', full_page=True)
            page.set_viewport_size({'width': 1440, 'height': 900})
            page.locator('#settings-done').click()
            # Restart the real host; SQLite retains all settings and no environment keys are needed.
            host.send_signal(signal.SIGINT); host.wait(timeout=15)
            host = subprocess.Popen([str(BINARY), '--port', str(port)], env=env, stdout=host_log, stderr=host_log)
            for _ in range(100):
                try:
                    with urlopen(url+'/health', timeout=.2): break
                except Exception: time.sleep(.05)
            page.reload()
            page.wait_for_function("() => document.querySelector('#connection').textContent === '本地已连接'")
            restored = page.request.get(url+'/api/settings').json()
            assert restored['status']['text_missing'] == []
            with sqlite3.connect(root/'data'/'settings.sqlite3') as database:
                stored = json.loads(database.execute('SELECT document FROM settings WHERE id=1').fetchone()[0])
            assert stored['stt']['provider'] == ''
            assert stored['stt']['api_key'] == ''
            assert stored['tts']['api_key'] == stored['text_model']['api_key'] == 'test-fixture'
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
            # Saving during a live conversation does not stop it; the next start reads the new value.
            page.locator('#settings').click()
            page.wait_for_function("() => !document.querySelector('#save-settings').disabled")
            page.locator('#service-settings details').last.evaluate('(n) => n.open = true')
            page.locator('#setting-instructions').fill('UPDATED_CONFIG_AFTER_SAVE')
            page.locator('#save-settings').click()
            page.get_by_text('已保存，下次启动对话时生效。正在进行的对话继续使用原配置。', exact=True).wait_for()
            page.locator('#settings-done').click()
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
            assert state['model_paths'][:2] == ['/chat', '/chat']
            assert state['model_paths'][2:] == ['/chat', '/chat']
            assert all('UPDATED_CONFIG_AFTER_SAVE' not in text for text in state['instructions'][:2])
            assert state['instructions'][2:] == ['UPDATED_CONFIG_AFTER_SAVE']*2
            assert THOUGHT not in page.locator('#messages').inner_text()
            assert all(THOUGHT not in text for text in state['speech'])
            resumed = json.loads(snapshot_path.read_text())
            assert len(resumed['sessions']) == 1 and resumed['sessions'][0]['id'] == original_id
            assert len([m for m in resumed['messages'] if m['role'] == 'User' and not m['acp_session_id']]) == 2
            page.locator('#new-chat').click()
            assert page.locator('#welcome').is_visible()
            assert page.locator('.session-item').count() == 1
            assert not errors, errors
            browser.close()
        print('PASS: local STT without cloud settings + bundled VAD default + required field markers + empty inputs without placeholders + browser settings save/errors/clear/redaction + HTTP origin/size validation + SQLite restart persistence + settings applied on next start + real WebSocket + Core + OpenAI-compatible adapter + two queued speak calls + actual browser playback reports + host persistence + reload/history/new conversation')
        print(f'Observed {state["models"]} model requests, 4 synthesis requests, 4 completed playback items across the same resumed business session')
    except Exception:
        host_log.flush(); host_log.seek(0); print(host_log.read())
        raise
    finally:
        host.send_signal(signal.SIGINT)
        try: host.wait(timeout=15)
        except subprocess.TimeoutExpired: host.kill(); host.wait()
        host_log.close(); mock.shutdown()
