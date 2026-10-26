# furica-host

单用户、本机回环 HTTP 宿主。复用 Core 的会话、模型工具循环、任务和播放规则，
由外壳负责音频设备、权限、AEC、重采样和播放。源码仅依赖既有模块的公开接口。

## 启动

`model`、`acp`、`speech.stt`、`speech.tts` 和 `speech.vad_model` 均可省略或设为 `null`。
不启用任何推理能力时，可使用 `config.minimal.example.json`，只配置 HTTP 与程序工作区；
Host 仍提供健康检查、配置和会话记录管理，文字输入返回 `model_unavailable`，语音入口返回 `voice_unavailable`。
配置检查集中在 Host，只检查已启用能力需要的参数；Core 接收上层准备好的运行参数并向外报告执行错误。

需要文字对话时，填写 `config.example.json` 中的实际模型协议、端点、模型名与凭证环境变量；
需要 ACP 时再填写 Agent 启动配置。示例中的 `.example` 域名和路径是占位值。
STT 与 TTS 可分别配置：连续语音输入需要 VAD、STT 和会话模型，TTS 用于可选的 `speak` 输出。
只有配置会话模型时才需要 `<data-directory>/system-prompt.toml` 和模型凭证；
不配置模型时不会读取提示词文件。所选模型必须支持已有的工具调用契约。
下面的命令演示启用会话模型的启动方式。

```sh
mkdir -p /absolute/path/to/host-data
cp furica-host/system-prompt.toml /absolute/path/to/host-data/system-prompt.toml
export FURICA_HOST_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export FURICA_MODEL_KEY='YOUR_PROVIDER_KEY'
cargo run -p furica-host -- /absolute/path/to/host-data --initialize /absolute/path/to/config.json
```

后续启动省略 `--initialize`，从 SQLite 读取配置；已有配置时携带初始化文件会被拒绝。
访问令牌至少 32 字节，所有凭证仅从配置引用的环境变量读取。
Host 为数据目录设置 Unix 0700 权限。
`configuration.sqlite3` 同时保存配置与会话记录，不能作为网站静态目录公开。
进程通过 `.host.lock.sqlite3` 的排他锁独占数据目录，第二个进程在访问数据库前被拒绝，进程退出自动释放。
Ctrl-C / SIGTERM 会停止接入、释放语音、等待 Core/ACP 清理并接收并提交剩余消息；失败以非零状态退出。

`GET /configuration` 查询 SQLite 中保存的配置；`GET /configuration/active` 查询最近一次成功发布的配置。
`PUT /configuration` 先校验并构建候选实例，再中断旧推理、语音和 ACP 任务、保存最终状态，
事务保存配置并切换整套实例。成功返回 `{"saved":true,"applied":true,"restart_required":false}`。
`POST /configuration/reload` 从 SQLite 重新读取配置并走相同应用流程，不改写配置表；成功返回
`{"applied":true,"restart_required":false}`。修改 SQL 或提示词文件后需显式调用，不自动监听文件或数据库。

切换期间拒绝新的业务操作，`GET /health` 返回 `ready:false, updating:true`。
切换完成后解除当前会话选择；业务记录保留，已中断任务不自动恢复或重放，语音通道需要重新打开。
配置、提示词、凭证或实例构建失败不影响旧任务。停止旧任务后发生清理或持久化失败时，
Host 保持未就绪，不发布候选实例；排除故障后再次 PUT 或 reload 可重建运行状态。
更新由 Host 持有的工作任务完成，HTTP 调用方断开不会放弃已经开始的切换。
配置表 `host_configuration(key, value)` 按字段存储：key 使用点分路径（如 `model.endpoint`），
value 使用 JSON 编码，数组作为一个值保存。读取缺失字段时使用代码中已有的默认值，
读取操作不将默认值写回数据库；非法值或缺失必要字段仍报配置错误。
整份配置保存采用事务替换，因此切换协议不会遗留旧协议字段。
数据库版本为 4，只支持新建空数据库或打开版本 4；旧版本直接拒绝，不提供迁移或旧 JSON 导入。
直接修改数据库只改变保存配置，不会立即更新运行实例；显式 reload 才应用。
模型、STT/TTS、ACP、提示词以及相关运行限制支持整体重建。
以下启动参数变化返回 HTTP 409、`restart_required` 错误码和具体字段说明，并拒绝整次 PUT（不保存、也不部分应用）：
`http.listen`、`http.token_env`、`http.allowed_origins`、`workspace`、`limits.channel_capacity`、`limits.context_max_bytes`。
显式 reload 遇到这些变化也会拒绝应用，但不撤销外部已经写入的 SQL。
凭证重新读取当前进程的环境变量，无法取得其他进程后来设置的环境变量。
更换模型后，已有会话的供应商 continuation 仍须通过既有来源校验；不兼容时请创建新会话，不自动迁移历史。
启动时校验配置、加载选中能力和本地文件；云端连接/账号是否有效仍需真实调用验证。

## 系统提示词

数据目录中的 `system-prompt.toml` 是系统提示词的唯一来源，只包含一个字符串 K-V：

```toml
system = '''
你是 Furica。默认使用 {{language}}，解释时间时采用 {{timezone}} 时区。
当前工作目录为 {{workspace}}。
'''
```

文件中的多行文本可直接手动编辑，示例还包含系统描述、功能调用策略与结果解释规则。
配置会话模型时，文件在启动和每次 PUT/reload 时读取，即使配置相同也重新读取；文件必须存在、使用 UTF-8、大小不超过 1 MiB。
格式错误、额外键、非字符串值、空白提示词、未知或未闭合变量会阻止启动。
渲染后的文本仍受 `limits.max_context_bytes` 约束，模型请求还会统一计算历史和工具定义的预算。

所有变量值和一次替换逻辑集中在 `src/configuration/application/prompts.rs::render_system`：
`language` 默认“简体中文”，`timezone` 默认 `Asia/Shanghai`，`workspace` 来自现有 HostConfig。
增加变量或调整默认语言/时区时只需修改该函数；变量值中的模板标记不会再次展开。
这里的时区提示只影响模型解释时间的约定，不实现时钟或定时任务。
core 继续接收渲染后的字符串，不读取 TOML 文件。工具参数和可用性仍以本次请求的实际工具定义为准。

`HostConfig.instructions` 已移除，新的初始化 JSON 与配置 API 不再接受该字段。
读取现有版本 4 数据库时，会忽略遗留的 `instructions` 配置项，下次保存完整配置时将其移除；
旧提示词如需继续使用，请手动迁入 TOML。系统提示词不再作为 host 配置写入数据库，
但 core 既有的 System 消息记录仍会保存实际使用的提示词。

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
| `GET /configuration` | 查询数据库中保存的配置 |
| `GET /configuration/active` | 查询最近一次成功发布的配置 |
| `PUT /configuration` | 保存配置并重新实例化，允许中断旧任务 |
| `POST /configuration/reload` | 重读数据库和提示词，重新实例化 |
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
每次分页在同一数据库读事务内取得条目和总数；不同请求间数据可能变化。
错误为 `{code,message,retryable}`，不会透传供应商响应或凭证。
本机应用需自行持续运行进程；Host 不安装系统服务。

## 持续语音

`speech` 中的 `vad_model`、`stt`、`tts` 分别可选。连续语音输入启用时需要 `vad_model` 和 `stt`，
`tts` 可以省略，此时仅显示文字回复。已配置的本地模型路径必须为绝对路径。
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

Host 独占数据库连接、结构、读写、事务和历史查询。Core 不接收仓储接口。
SQLite 按 `sessions`、`messages`、`acp_sessions`、`runs`、`tool_calls`、`questions`
分别组织记录；`received_messages` 保存每个运行消息流最后一组的序号和内容。
Core 的完整记录与接收凭据在同一事务提交，再调用 `acknowledge_messages(sequence)`。
相同序号和内容可重复提交，冲突或断序会拒绝。接收工作持有串行锁直到提交及确认结束，
调用方取消不会释放尚在工作的锁。Prepared Run 在提交前不会启动。
写盘失败保留 Core 待交付数据，标记未就绪，每轮最多尝试三次并推送故障；可通过 `flush` 重试。
提交后确认失败返回 `message_acknowledgement_failed`，已提交记录不会回滚。
SQLite 使用 `synchronous=FULL`；配置事务与会话消息事务独立。

Host 启动时逐会话加载并归一化历史；未完成执行中断、问题取消，不重放副作用。
选择会话时仅从数据库加载目标会话的完整 `SessionData`，由 Core 留在内存供运行使用。
`context_max_bytes` 限制单会话导入的记录内容，默认 64 MiB；模型请求仍受独立的 `max_context_bytes` 限制。
历史列表直接从数据库分页读取，查询前接收当前 Core 已产生的待交付消息。
不再在每次变更时复制、序列化或覆盖全部会话快照。

可靠交付覆盖当前进程内的有序接收、失败重试和事务去重。Core 未交付队列保存在内存，
进程异常退出前尚未提交的输出不能恢复；已提交记录恢复时不会自动重放 ACP 副作用。
音频沿用 `PlaybackChunk` 和实际播放反馈，不新增音频存档或断点续传。
不支持多个业务会话同时运行、可靠 SSE 日志或设备控制。
SSE 滞后/重连后查询最新状态。

## 代码职责

`facade/http` 负责 HTTP、鉴权、响应投影与音频编码；`facade/application` 负责 Shell 用例及 Core 生命周期协调。
共享运行状态只在 facade 应用层内部访问，切换与归档复用清理和重建流程。
`infrastructure/sqlite` 统一负责配置及会话记录读写；`infrastructure/audio` 和 `model_policy` 保留必要的能力适配。
`configuration` 定义并校验配置；`infrastructure/components` 持有配置及对应实例，统一构建与替换。
`bootstrap` 调用该构建入口，负责数据目录锁及启动恢复；`facade/application/configuration` 协调任务中断和配置应用。
嵌入式调用改用 `Host::new(components, database_path)`；注入自有 backend 时使用 `Components::new(...)`。
配置读取改为 `host.config()`；HTTP 启动使用 `http::serve(listener, host, token, shutdown)`，配置存储由 Host 自行持有。
不再单独建立 Host 的 interaction、sessions、voice 领域包装模块；公开 HTTP 路径保持兼容。

## 验证

```sh
cargo test -p furica-host --offline
cargo clippy -p furica-host --all-targets --offline -- -D warnings
cargo test -p furica-host --offline --test vad real_silero -- --ignored
```

本次热更新回归使用 `--lib` 和 `bootstrap`、`capabilities`、`hot_reload`、`http`、`interaction`、
`storage`、`vad`、`voice` 测试目标，共 43 项通过、1 项真实模型测试忽略；对应目标 Clippy 通过。
当前工作区的 `credentials` 测试依赖尚未接入的明文凭证接口（如 `save_update`、`http.token`），
其编译错误在本轮开始前已经存在，因此上述全量 test/all-targets 命令仍不能通过。

HTTP 测试需要本机回环监听权限，并明确绕开环境代理。
测试替身覆盖协议、任务保存确认、故障和资源生命周期；微型 ONNX 夹具只验证输入适配。
单独的真实 Silero 测试验证模型加载/静音输入，不代表识别效果或云端时延验收。
真实模型回复、STT/TTS 效果、云端凭证和外壳设备仍需在接入环境验收。

对象定义与决策记录见本目录 `DESIGN.md`。
