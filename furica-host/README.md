# furica-host

单用户、本机回环 HTTP 宿主。复用 Core 的会话、模型工具循环、任务和播放规则，
由外壳负责音频设备、权限、AEC、重采样和播放。源码仅依赖既有模块的公开接口。

## 启动

复制本目录 `config.example.json`，填写绝对工作目录、实际模型协议/端点/模型名和 ACP agent 可执行文件。
示例中的 `.example` 域名和路径是占位值，不能直接用于真实调用。
配置 `speech: null` 时文字交互可独立工作，开启语音返回 `voice_unavailable`。
所选模型必须支持已有的工具调用契约；配置不隐式猜测供应商、账号或模型能力。

```sh
export FURICA_HOST_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export FURICA_MODEL_KEY='YOUR_PROVIDER_KEY'
cargo run -p furica-host -- /absolute/path/to/host-data --initialize /absolute/path/to/config.json
```

后续启动省略 `--initialize`，从 SQLite 读取配置；已有配置时携带初始化文件会被拒绝。
访问令牌至少 32 字节，所有凭证仅从配置引用的环境变量读取。
Host 为数据目录设置 Unix 0700 权限。
`configuration.sqlite3` 同时保存配置与会话快照，不能作为网站静态目录公开。
进程通过 `.host.lock.sqlite3` 的排他锁独占数据目录，第二个进程在读取/保存快照前被拒绝，进程退出自动释放。
Ctrl-C / SIGTERM 会停止接入、释放语音、等待 Core/ACP 清理并保存最终快照；失败以非零状态退出。

`GET/PUT /configuration` 查询/事务保存下次启动配置，返回 `saved` 和 `restart_required`。
配置表 `host_configuration(key, value)` 按字段存储：key 使用点分路径（如 `model.endpoint`），
value 使用 JSON 编码，数组作为一个值保存。读取缺失字段时使用代码中已有的默认值，
读取操作不将默认值写回数据库；非法值或缺失必要字段仍报配置错误。
整份配置保存采用事务替换，因此切换协议不会遗留旧协议字段。
数据库版本 1 的整份 JSON 配置会在启动时事务转换为 k-v 表；版本 1/2 均升级至版本 3，新增会话快照表。
修改数据库不热更新当前组件、凭证、监听地址或 CORS 名单。
启动时校验配置、加载选中能力和本地文件；云端连接/账号是否有效仍需真实调用验证。

## HTTP 交互

所有业务请求都使用 `Authorization: Bearer $FURICA_HOST_TOKEN`，包括 `/health` 和 SSE。
监听地址仅允许回环 IP。Host 头须为实际监听地址或 `localhost:端口`。
浏览器 Origin 需精确写入 `http.allowed_origins`，不能使用 `*`；无 Origin 的本机客户端仍需令牌。
浏览器通过 fetch 读取 SSE/音频，以便携带请求头；不要将令牌放在 URL 中。

```sh
curl -H "Authorization: Bearer $FURICA_HOST_TOKEN" http://127.0.0.1:8765/health
curl -X POST -H "Authorization: Bearer $FURICA_HOST_TOKEN" http://127.0.0.1:8765/sessions
curl -H "Authorization: Bearer $FURICA_HOST_TOKEN" -H 'Content-Type: application/json' \
  -d '{"text":"你好"}' http://127.0.0.1:8765/sessions/SESSION_UUID/text
```

输入返回 202 只表示已进入 Core。通过 `/events` 与查询接口获取结果。
首次文字或语音输入自动选择业务会话；其他会话须先 `POST /interaction/session {"session_id":"…"}`。
有语音占用、非终态任务或待回答问题时拒绝切换。允许切换时中断当前模型回复，
关闭空闲 ACP 连接并保存后重建运行，保留会话历史；不承诺保留旧 ACP 进程。
语音开启/关闭会重连当前后端以切换 speak 策略，允许中断当前模型回复，已经启动的任务仍保留。
同一选中会话的输入交给 Core 现有队列；队列满显式返回 429，不建立另一份模型历史。

| 路由 | 行为 |
| --- | --- |
| `GET /health` | 就绪、存储、交互状态、播放版本、语音能力和限制 |
| `GET/PUT /configuration` | 配置查询与保存；需重启生效 |
| `POST/GET /sessions` | 创建/分页列表 |
| `GET/PATCH /sessions/{id}` | 状态/改名 `{ "name": "…" }` |
| `POST /sessions/{id}/archive` | 归档；当前语音或任务占用时拒绝 |
| `GET /sessions/{id}/messages` | 按会话 sequence 排序的消息投影，排除模型原始 continuation |
| `GET /sessions/{id}/{acp-sessions,runs,tool-calls,questions}` | 分页查询；不存在直接 ACP 控制入口 |
| `POST /interaction/session` | 选择会话 |
| `POST /sessions/{id}/text` | 提交文字 `{ "text": "…" }` |
| `POST /interaction/interrupt` | 打断生成及播放 |
| `GET /events` | SSE 通知；滞后交付 `resync`，重新查询状态 |
| `POST /voice` | 开启唯一通道 `{ "session_id": "…", "owner": "…" }` |
| `DELETE /voice/{id}` | 关闭语音，不调用全局任务 shutdown |
| `POST /voice/{id}/heartbeat` | 独立所有者心跳 |
| `PUT /voice/{id}/audio/{sequence}` | 二进制音频批次 |
| `GET /voice/{id}/audio` | 唯一音频下载消费连接 |
| `POST /voice/{id}/playback` | 实际播放位置 |

分页查询使用 `offset`、`limit`，默认 50、上限 200；响应 `{items,offset,limit,total}`。
每次分页基于一致快照；不同请求间数据可能变化。
错误为 `{code,message,retryable}`，不会透传供应商响应或凭证。
本机应用需自行持续运行进程；Host 不安装系统服务。

## 持续语音

`speech` 非空时指定 `vad_model`、`stt`、`tts`。路径必须为绝对路径。
VAD 使用现有 Silero 默认触发策略，输入/输出均固定单声道 16 kHz PCM16 小端。
不接声纹门控；所有者主动开启。此选择已经在实施契约中确认。

STT 的 `provider` 支持 `sensevoice`（`model_path`、`tokens_path`）、
`aliyun`（`endpoint`、`model`、`api_key_env`）、
`volcengine`（再加 `resource_id`）。
TTS 的 `provider` 支持 `aliyun` / `volcengine`，字段为 `endpoint`、`model`、`voice`、`api_key_env`，
Volcengine 再加 `resource_id`。Host 传入 16000 输出采样率；不兼容的配置会报错。
本地模型不由 Host 下载。

开启响应包含 `channel_id` 和仅返回一次的随机 `voice_token`。
之后的通道请求均携带 `X-Voice-Token`。整个进程最多一个通道，并发开启只有一个成功。
默认每 5 秒心跳，15 秒超时释放；重开获得新 UUID/token，旧请求返回 410，历史保留。

上传正文为 `application/octet-stream`，每批 1–3200 样本，建议 1600 样本（100 ms）。
从 sequence=0 开始，一次只有一个请求在途。
成功响应 `{sequence,next_sequence,duplicate}` 表示 Core 输入队列已接收，后续推理仍可能失败。
仅当前通道上一已接收批次可原样重试；同号不同内容/断序返回 409。
429 后只重试当前批次，默认最多 3 次且最长 2 秒，不能无限积压新音频。
Host 保留有限去重和重试数据；超过预算结束通道。进程重启不重放音频。

Silero 在独立阻塞工作中按 512 样本推理，跨上传批次保存余量。
Host 按确认的样本起止位置保留完整语音段，再交付既有级联后端。
段长度受 `max_utterance_ms` 限制，VAD 确认延迟也有有界保留；不开另一套语音识别历史。
SpeechStarted 引起的 Core 打断先于该段 Ended 交付，避免迟到打断取消新回复。
关闭会丢弃未完成段和不足 512 样本尾部；重开重新加载并重置该代 VAD。

音频下载为连续消息，每条是 `u32le header_length` + UTF-8 JSON header + `samples*2` 字节 PCM。
header 包含 `channel_id,generation,item_id,content_index,sample_rate,samples`；最大 4096 字节，
单条 PCM 最多一秒。HTTP 网络分块不是消息边界。
SSE/健康查询报告当前播放 generation；变化时立即停止并清空旧版本，播放前再次检查版本。
普通状态订阅者不消费音频。音频连接中断、队列溢出或心跳超时后需重开，不补播旧音频。

实际播放后调用 playback，提交 `{generation,item_id,content_index,played_ms}`。
不得将下载完成当作播放完成。Core 校验单调进度与已交付长度；旧 generation 的反馈按既有契约忽略。
Host 不操作设备、不重采样，不伪造完成反馈。

## 保存与限制

配置和会话快照统一使用 SQLite 事务与数据库迁移版本；未知未来版本拒绝打开。
会话表 `session_snapshot(id, document)` 只保存 id=1 的当前快照，document 为完整 Core SessionData JSON，默认上限 64 MiB。
保存先取得串行锁再捕获快照，事务提交成功后仅 acknowledge_saved 该份快照；取消调用方不会释放尚在提交的保存锁。
写盘失败不确认 Prepared Run，标记未就绪，每轮最多尝试三次并推送故障。
提交后的 Core 确认失败单独报告 `snapshot_acknowledgement_failed`，已提交的数据不会回滚。
SQLite 使用 `synchronous=FULL`；配置更新与会话保存分别提交，互不覆盖对方数据。

启动先读取数据库快照；仅当快照记录不存在时，校验并事务导入旧 `sessions.json`，缺失旧文件则保存空快照。
导入失败停止启动，修复后可重试。旧文件保留但不再写入；数据库记录一旦存在便是唯一恢复来源。
数据库快照损坏时明确报错，不回退到旧文件。恢复继续使用 Core 的中断/取消规则，不重放旧任务副作用。

会话查询和保存需要通过 Core 完整快照接口复制数据；适合首期本机数据规模。
不支持多个业务会话同时运行、运行配置热更新、可靠 SSE 日志、音频断点续传或设备控制。
SSE 滞后/重连后查询最新状态；不要把 SSE 当作持久事件日志。

## 代码职责

`facade/http` 负责 HTTP、鉴权、响应投影与音频编码；`facade/application` 负责 Shell 用例及 Core 生命周期协调。
共享运行状态只在 facade 应用层内部访问，切换与归档复用清理和重建流程。
`infrastructure/sqlite` 统一负责配置及快照读写；`infrastructure/audio` 和 `model_policy` 保留必要的能力适配。
`configuration` 定义并校验配置，`bootstrap` 负责组装、数据目录锁及启动恢复。
不再单独建立 Host 的 interaction、sessions、voice 领域包装模块；公开 HTTP 路径保持兼容。

## 验证

```sh
cargo test -p furica-host --offline
cargo clippy -p furica-host --all-targets --offline -- -D warnings
cargo test -p furica-host --offline --test vad real_silero -- --ignored
```

HTTP 测试需要本机回环监听权限，并明确绕开环境代理。
测试替身覆盖协议、任务保存确认、故障和资源生命周期；微型 ONNX 夹具只验证输入适配。
单独的真实 Silero 测试验证模型加载/静音输入，不代表识别效果或云端时延验收。
真实模型回复、STT/TTS 效果、云端凭证和外壳设备仍需在接入环境验收。

对象定义与决策记录见本目录 `DESIGN.md`。
