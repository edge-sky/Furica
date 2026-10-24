# furica-host 实施契约

本文件将 `docs/host-module-proposal.md` 的已确认行为收敛为可实施的对象和协议建议。
运行与接入策略、新增对象、字段及补充限制已于本轮对话确认，用户授权按本契约实施。

## 1. 模块职责与复用

Host 直接复用 Core 的 Session、Message、AcpSession、Run、ToolCall、Question、SessionData，
Core 保留业务规则与工具执行，Host 组织持久化，不增加第二套业务实体。

- facade/http：HTTP 路由、鉴权、请求解析、响应投影、SSE 及音频传输编码。
- facade/application：Shell 用例、运行实例生命周期、会话切换、语音通道与持久化协调。
- configuration：配置定义、校验及 Core/适配器配置转换。
- infrastructure/sqlite：共用数据库连接策略与版本迁移，分别实现配置及会话记录读写。
- infrastructure/audio、model_policy：保留必要的 VAD 和模型调用策略适配。
- bootstrap：组件装配、目录独占锁、启动加载。

复用现有 Host、HostState、Runtime 和 SessionStore。HostState/Runtime 及其字段限制在 facade 应用层内；
bootstrap 通过方法移交目录锁。切换会话与归档当前会话共用清理和重建流程，保留原来的串行边界。
HTTP 投影排除模型原始 continuation。PlaybackPosition、PlaybackChunk 和 SpeechActivity 复用 Core 语义。

## 2. 配置对象

启动参数仅指定数据目录和可选初始化配置 JSON 路径，不在命令行传递凭证。
进程在访问会话数据库前，通过 `.host.lock.sqlite3` 的排他事务独占数据目录，防止重复启动覆盖正在使用的数据；退出自动释放锁。
配置和会话记录共同存入 `configuration.sqlite3`；不导入旧 `sessions.json`。
首次启动必须提供初始化配置；数据库存在时从数据库加载，拒绝用初始化文件隐式覆盖。

`HostConfig` 由以下部分组成；JSON 字段为 snake_case，拒绝未知字段，时间字段统一使用整数毫秒：

| 部分/对象 | 字段及含义 |
| --- | --- |
| `HostConfig` | `http`、`workspace`（绝对路径）、`model`、`speech`（可空）、`acp`、`limits`、`instructions`（可空） |
| `HttpConfig` | `listen`（回环 IP 与端口）、`token_env`（非空环境变量名）、`allowed_origins`（完整 origin 列表，默认空） |
| `ModelConfig` | `protocol`（`openai_compatible` / `openai_responses` / `anthropic`）、`endpoint`、`model`、`isolation_scope`、`api_key_env`、`connect_timeout_ms` |
| 模型协议选项 | compatible：`max_tokens_field`、`supports_reasoning_effort`；responses：`supports_reasoning`；anthropic：`version`、`supports_adaptive_thinking`、`supports_thinking_budget`。按 protocol 定义带标签枚举，不接受不适用字段 |
| `SpeechConfig` | `vad_model`（绝对 ONNX 文件路径）、`stt`、`tts`。整体为空时支持文字，语音开启返回能力不可用；不伪造 STT/TTS 结果 |
| `SttConfig` | 带标签枚举：`sensevoice` 使用 `model_path`、`tokens_path`；`aliyun` 使用 `endpoint`、`model`、`api_key_env`；`volcengine` 使用前述云端字段及 `resource_id` |
| `TtsConfig` | 带标签枚举：`aliyun` 使用 `endpoint`、`model`、`voice`、`api_key_env`；`volcengine` 再含 `resource_id`。输出采样率固定为本轮已确认的 16000，由 Host 传入现有配置并检查 |
| `AcpConfig` | `command`（绝对可执行文件路径）、`agent_id`、`args`、`auth_method`（可空）、`handshake_timeout_ms`、`run_timeout_ms`、`cancel_timeout_ms`、`idle_timeout_ms`。映射到现有适配器 AcpConfig，不复制执行协议 |
| `RuntimeLimits` | `channel_capacity`、`connect_timeout_ms`、`idle_timeout_ms`、`inference_timeout_ms`、`max_history_messages`、`max_context_bytes`、`max_output_tokens`、`max_utterance_ms`、`max_generated_audio_ms`、`max_acp_sessions`、`max_runs`、`question_timeout_ms`、`heartbeat_timeout_ms`、`upload_retry_limit`、`upload_retry_window_ms`、`context_max_bytes` |

与已有对象对应的默认值和校验边界沿用 Core/适配器默认值；不新增推理参数模型，首期 reasoning 为 None。
Host 特有默认值：监听 `127.0.0.1:8765`、心跳建议每 5 秒，15 秒失联释放；
当前批次最多重试 3 次、最长 2 秒；单会话导入上限 64 MiB；历史每页默认 50、最多 200 条。
上传单批 1–3200 个样本（最长 200 ms），建议 1600 个样本（100 ms）。所有有界队列容量复用 `channel_capacity`。
VAD 首期直接使用已有 VadConfig 默认值，不新增外部调参对象。

不提供模型、云端地址、账号或 ACP 命令的隐式猜测值；选中的必需配置缺失即报错。
只有选中的能力在启动时加载凭证及本地模型；本地 ONNX 推理不在 HTTP 或 Core ingress actor 运行。

SQLite 使用 `PRAGMA user_version=4`，只支持新建空库或打开版本 4，不兼容历史版本。
配置采用 `host_configuration(key,value)`；会话采用 sessions、messages、acp_sessions、runs、tool_calls、questions 六张记录表。
各记录保留 ID、session_id、sequence 和完整实体 document，并按所属会话建立索引。
`received_messages(stream,sequence,document)` 保存每个运行流最后的事务接收凭据。
配置读取不写回默认值；配置保存使用独立事务，避免协议切换遗留字段。
连接使用 synchronous=FULL 和有界 busy timeout。目录锁保存在独立锁库。

配置更新先验证结构和静态边界，再提交；报告 `restart_required=true`，不热应用。
配置中仅保存凭证环境变量名；访问令牌与供应商密钥不写数据库、事件或日志。

## 3. HTTP 公共对象

所有非 CORS 预检请求使用 `Authorization: Bearer <token>`，包括健康查询和 SSE；
语音端点另要求 `X-Voice-Token`。浏览器使用 fetch 读取 SSE 与音频，令牌不放 URL。
只接受实际监听地址/localhost 的 Host 头；Origin 必须在配置的精确名单中，无 Origin 的本机客户端可携带令牌访问。
禁止任意来源 CORS；预检只允许实际支持的方法和请求头。

| 对象 | 字段/结构 |
| --- | --- |
| `ApiError` | `code`（稳定错误码）、`message`（安全错误说明）、`retryable`（布尔）；不透传凭证或供应商原始响应 |
| `HealthView` | `ready`、`storage`（`healthy` / `failed`）、`interaction_state`、`active_session_id`（可空）、`voice_available`、`voice_channel_id`（可空）、`playback_generation`、`playback_active`、`sample_rate=16000`、`channels=1`、`encoding=pcm_s16le`、`max_batch_samples=3200`、`heartbeat_interval_ms=5000`、`heartbeat_timeout_ms`、`upload_retry_limit`、`upload_retry_window_ms` |
| `SaveConfigResult` | `saved=true`、`restart_required=true` |
| `SessionView` | 复用 Core Session 的 `id`、`name`、`state`、`created_at_ms`、`updated_at_ms` |
| `RenameSession` | `name` |
| `SelectSession` | `session_id` |
| `TextInput` | `text`（非空，至多 64 KiB，同时受 Core 上下文边界限制） |
| `Accepted` | `accepted=true`；只表示已经进入 Core，不表示模型或任务完成 |
| `PageQuery` / `Page<T>` | 查询 `offset`、`limit`；响应 `items`、`offset`、`limit`、`total`。每次查询的一致快照内分页，不承诺跨请求的不可变游标 |
| `MessageView` | `id`、`session_id`、`acp_session_id`、`run_id`、`sequence`、`role`、`source`、`state`、`created_at_ms`、`updated_at_ms`；`content` 投影为 `kind` 与 `text` / `parts` / `tool_call_id` / `blocks`，模型 parts 复用 ModelContent，排除 continuation |
| `SessionStatusView` | `session`、`interaction_state`、`selected`；ACP 上下文、Run、ToolCall、Question 分别通过分页子资源查询，复用 Core 可序列化对象，不返回全量 SessionData |
| `OpenVoice` | `session_id`、`owner`（外壳自选非空标识，至多 128 字节，用于诊断，不当作授权） |
| `VoiceLeaseView` | `channel_id`（每次开启新 UUID）、`voice_token`（仅开启成功响应返回）、`session_id`、`sample_rate`、`channels`、`encoding`、`next_sequence=0`、`heartbeat_interval_ms`、`heartbeat_timeout_ms` |
| `UploadReceipt` | `sequence`、`next_sequence`、`duplicate`。批次号使用路径中的 u64，正文为二进制 PCM，Content-Type 为 application/octet-stream |
| `PlaybackReport` | `generation`、`item_id`、`content_index`、`played_ms`，映射为 Core PlaybackPosition；仅真实播放后上报 |
| `HostEvent` | 带 `kind` 标签，包含以下变体所需字段：`state`（session_id/state）、`transcript`（session_id/user/text/final_chunk）、`sessions_changed`（无快照）、`playback`（channel_id/generation/active）、`voice_closed`（channel_id/reason）、`storage`（healthy 布尔）、`error`（code/message）、`resync`（要求重新查询） |

HTTP 错误基本映射：400 参数/格式错误；401 凭证错误；403 Origin/Host 拒绝；404 业务资源不存在；
409 占用、模式/会话冲突、重复内容冲突、断序；410 已失效通道；413 超出大小；429 有界队列背压；
503 能力、持久化或运行暂不可用；504 激活/关闭超时。公共错误仅返回稳定说明，不透传供应商原始响应或凭证。

## 4. 路由建议

| 路由 | 用途 |
| --- | --- |
| `GET /health` | 健康/就绪/能力和播放恢复查询 |
| `GET/PUT /configuration` | 查询/事务保存配置，不热更新 |
| `POST/GET /sessions` | 创建/分页列表 |
| `GET/PATCH /sessions/{id}` | 状态查询/改名 |
| `POST /sessions/{id}/archive` | 归档；当前会话存在语音或非终态任务时拒绝 |
| `GET /sessions/{id}/messages` | 分页消息投影 |
| `GET /sessions/{id}/{acp-sessions,runs,tool-calls,questions}` | 分页执行状态查询，不提供直接 ACP 操作 |
| `POST /interaction/session` | 显式选择/切换当前业务会话 |
| `POST /sessions/{id}/text` | 向选中会话提交文字；未选中会话时首次提交自动选择 |
| `POST /interaction/interrupt` | 打断当前生成及播放，复用 Core 语义 |
| `GET /events` | SSE，仅状态观察，不消费音频；无完整事件历史重放 |
| `POST /voice` | 原子预留并开启唯一语音通道 |
| `DELETE /voice/{channel_id}` | 结束语音通道，允许中断当前回复，不关闭全局任务 |
| `POST /voice/{channel_id}/heartbeat` | 所有者存活反馈 |
| `PUT /voice/{channel_id}/audio/{sequence}` | 有界二进制上传；同一通道一次一个上传请求在途 |
| `GET /voice/{channel_id}/audio` | 唯一下载消费连接；断线后结束通道，重新开启不补播 |
| `POST /voice/{channel_id}/playback` | 真实播放反馈 |

音频下载采用长度前缀消息：`u32 little-endian header_length` + UTF-8 JSON header + PCM16LE bytes。
header 为 `AudioEnvelope { channel_id, generation, item_id, content_index, sample_rate, samples }`；
`samples` 是本帧样本数，随后恰有 `samples * 2` 字节。JSON header 最大 4096 字节；
单音频消息最多一秒，与 Core 输出校验一致。HTTP 网络分块不表示消息边界。
播放版本变化通过 SSE 及健康查询交付，接收方必须在播放前检查版本并清空旧队列。
所有权 token 和通道 UUID 同时校验，旧通道的上传、下载、心跳和反馈均不能影响新通道。

## 5. 内部协调对象及状态

以下是实现必需的私有对象范围，按相应 DDD 目录组织；不增加第二套业务实体。
确认后可按实现需要拆分同职责字段及 HTTP 提取器，不扩大对象职责或改变对外契约。

| 对象/职责 | 保留数据与状态 |
| --- | --- |
| 配置存储 | SQLite 路径/连接；只负责迁移及配置事务，不依赖 HTTP handler |
| 会话存储与消息接收 | SQLite 路径、单会话导入上限、待交付通知、串行接收锁和存储健康状态。完整记录及接收凭据同事务提交后确认序号 |
| Host 运行协调 | 当前 SessionManager、已选 Session UUID、可空 CoreHandle/CoreOutput、运行 epoch、组件工厂、停止信号。切换及启停互斥，业务会话和 HTTP 连接不共享生命周期 |
| 文字策略适配器 | 原 TextModel 引用、共享的文字/语音模式标记；单次调用锁定模式，文字模式删除请求 tools 中的 speak，拒绝供应商流中任何不在本次允许集的 ToolStart；保持原 continuation_source，不修改历史、原始续接或工具结果 |
| 语音通道协调 | `Closed` / `Opening` / `Open` / `Closing`；通道 UUID、随机 owner token、业务会话 UUID、owner、最后心跳、期望批次号、上一已接受批次原始字节、当前失败批次及重试次数/起点、单消费者下载占用、取消令牌。整个 Host 只保留一份占用 |
| 上行去重 | 仅保存上一已接受批次的完整字节及序号；相同序号/字节返回 duplicate，内容不同拒绝；其他旧序号和未来序号拒绝。队列满不推进序号；同批次的背压重试计数有界，耗尽关闭通道 |
| 音频交付 | Core 音频接收器、当前通道 epoch、当前播放版本、有界输出通道、下载是否已认领。普通 SSE 订阅者无法取走音频；堵塞、无法交付或链路中断显式关闭语音 |
| VAD 后端组合适配器 | 已有 VoiceBackend 引用、Silero 模型/专用有界工作过程、跨批次 512 样本余量、绝对样本偏移、用于回溯边界的有界 PCM、当前语音段状态、取消与等待打断的状态；独立任务按序推理，不在同步 Vad::detect 中推理 |
| HTTP 应用状态 | 应用协调引用、访问凭据、允许的 Host/Origin 集合、事件广播引用；不包含业务规则复制 |
| 私有错误 | 配置/存储/Core/访问/冲突/背压/关闭/超时等必要分类，映射为稳定 ApiError；不建立通用错误框架 |

上行确认仍表示 `CoreHandle::audio` 接受成功，而非 VAD 或 ASR 成功。
Core capture sequence 在运行实例内单调递增；外部批次 sequence 每个新通道从 0 开始，两者分开管理。
上传握手、通道失效检查、Core 队列接收及去重确认在同一串行过程内，避免出现已接收但去重状态未更新的窗口。
对断序、内容冲突等可明确拒绝且未接收的请求不推进序号；客户端不能无限重试或堆积，HTTP 返回剩余预算所需的明确错误码。

VAD 是唯一边界来源。保留确认延迟对应的 PCM，按 Silero 返回的 start/end 样本位置切分，
转换成既有 Started/Continuing/Ended；不把上一推理帧的结果贴到当前输入。
SpeechStarted 触发 Core 现有打断时，组合适配器先等待并转发该打断，再允许该段 Ended 到达级联后端，
避免迟到打断取消新一轮识别/回复。所有等待受取消及既有超时约束。
正常结束/失联按已确认策略丢弃未完成的输入和不足 512 样本尾部，不补零制造语音；重开重置 VAD、余量、回溯和所有权。

## 6. 现有接口带来的切换限制（已确认）

`Core::start` 固定绑定 session_id，CoreHandle 无切换接口；Core 退出和 Drop 会清理共享 SessionManager。
证据：`furica-core/src/orchestration/application/runtime/startup.rs`、
`furica-core/src/orchestration/application/mod.rs`、`furica-core/src/orchestration/application/runtime/mod.rs`。

建议首期显式切换：有语音占用、非终态 Run 或 Pending Question 时返回 409，避免取消任务。
允许切换时，结束当前主模型回复，关闭旧 Core 及空闲 ACP 连接，等待清理完成，接收剩余消息，
再从数据库加载目标会话的 SessionData 建立新的 SessionManager/Core 运行代际；HTTP 服务、业务会话和历史保留。
若关闭或持久化失败，报告未就绪，不在旧管理器已关闭的情况下假装切换成功。
向非选中会话直接发文字返回 409，外壳须先显式选择；首次输入可自动选择。
切换不承诺保留原 ACP 进程连接；已有终态任务与消息保留，后续 ACP 控制仍仅由主模型工具完成。
归档当前会话遵循同样的安全清理路径。

同一选中会话的文字与音频通过一个运行实例和现有队列有序接入；开启或关闭语音会结束旧后端并重连，
使 speak 策略与外部通道状态同步，期间允许中断当前主模型回复；不会另外建立文字历史。
语音模式切换使用 Core 公开的 mute(false) 进入 Standby 并释放后端，保留待回答 ACP 问题；不调用会取消问题的 end_conversation 或全局 shutdown。ACP 任务保留。

## 7. 验证与交付

确认后先在新模块中验证公开接口组合，再实现完整 HTTP 闭环。
测试使用现有 trait 的测试替身；必要的替身私有状态限定为输入记录、预设事件、取消和故障注入，
不新增业务模型。回环 HTTP 测试覆盖访问校验、文字任务闭环、并发占用、去重/背压、播放版本、失联和退出。
存储测试覆盖新库初始化、旧库拒绝、记录与接收凭据的事务回滚、重复/断序投递、上下文大小和会话隔离。
消息接收失败不确认 Prepared Run；Core 保留待交付组供重试，数据库提交后才确认并调度。
取消调用方不打断已开始的提交与确认。确认失败返回 message_acknowledgement_failed，不声称数据库回滚。
启动从数据库逐会话加载，Core 归一化未完成工作，Host 接收归一化记录；不恢复旧进程或重放任务。
退出先停止接入、关闭语音、清理 Core/ACP，再接收剩余消息，退出结果反映持久化和清理失败。
Core 队列为内存队列，进程崩溃前尚未提交的数据不具备跨进程恢复保证。
模型效果、云端账号和真实设备时延不由协议测试替代。完整边界见 [数据流](../docs/session-data-flow.md)。
