# furica-adapters

Furica 的可替换云端语音与文本模型组件。宿主在启动时构造适配器并注入 Core；供应商凭证、服务模型、音色和协议消息留在适配器内。

当前实现：

| 可选功能 | STT | TTS |
| --- | --- | --- |
| `aliyun` | Fun-ASR-Realtime 非实时 HTTP：内存 WAV → Base64 Data URI → 最终文本 | CosyVoice WebSocket：完整文本 → 原生 PCM16 音频流 |
| `volcengine` | 豆包录音文件极速版 HTTP：内存 WAV → Base64 → 最终文本 | V3 单向 HTTP Chunked：JSON/Base64 → PCM16 音频流 |

默认功能为空，不引入 HTTP、WebSocket 或 ONNX 客户端。`http` 是通用 HTTP 依赖集合，`cloud` 在其上增加语音 Base64 封装依赖。Core 不依赖本 crate。本地 Silero VAD 和 SenseVoiceSmall STT 位于 `furica-local`；本 crate 继续提供云端 ASR。

```toml
[dependencies]
furica-adapters = { path = "../furica-adapters", features = ["aliyun", "volcengine"] }
```

## 文件职责

语音适配器按供应商划分模块，例如 `aliyun::{AliyunStt, SttConfig}`；
文本协议统一归入 `text_model`，例如
`text_model::openai_responses::{OpenAiResponses, Config}`。三个文本协议不再从 crate 根目录导出。
每个模块的 `mod.rs` 导出既有类型，`config.rs` 保存供应商配置，私有
`infrastructure/` 实现 Core 的能力端口。语音实现拆为 `stt.rs` 和 `tts.rs`；
文本实现拆为请求编码 `request.rs`、协议状态 `stream.rs`，以及 Responses/Anthropic
的原始续接检查 `continuation.rs`。

共用 HTTP、语音编码与文本 SSE 机制放在 crate 私有的 `infrastructure/`；
原有协议测试与回环夹具集中于 `src/tests/`。业务领域、能力契约和调用用例仍由
Core 拥有，具体目录与依赖方向见 [DDD 组织说明](../docs/ddd-organization.md#furica-adapters-的边界与组织)。

## 文本模型

| feature / 模块 | 适配器 | 线协议 |
| --- | --- | --- |
| `openai-compatible` / `text_model::openai_compatible` | `OpenAiCompatible` | Chat Completions |
| `openai-responses` / `text_model::openai_responses` | `OpenAiResponses` | Responses，stateless 原始 items 回传 |
| `anthropic` / `text_model::anthropic` | `Anthropic` | Messages，含签名 thinking / redacted thinking blocks |

三个模块分别使用自己的 `Config`，`Config::new(endpoint, api_key, model,
isolation_scope)` 接收完整 URL、凭证、模型和不含凭证的账号/租户隔离标识。
`connect_timeout` 默认 10 秒，调用方另设整体截止时间。端点只接受 HTTPS
（回环允许 HTTP），不接受 URL 内嵌凭证、query 或 fragment，不跟随重定向。
凭证配置不实现 Debug，认证头标记 sensitive，错误不会回显服务原始载荷。

```rust,no_run
# #[cfg(feature = "openai-responses")]
# async fn example() -> furica_core::Result<()> {
use furica_adapters::text_model::openai_responses::{Config, OpenAiResponses};
use furica_core::text_model::{self, ModelMessage, ModelRequest};
use tokio_util::sync::CancellationToken;
let config = Config::new(
    "https://api.openai.com/v1/responses".into(),
    std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY"),
    std::env::var("OPENAI_MODEL").expect("OPENAI_MODEL"),
    "my-account/project-a".into(),
);
let model = OpenAiResponses::new(config)?;
let response = text_model::generate(&model, ModelRequest {
    messages: vec![ModelMessage::User("Hello".into())],
    tools: vec![], instructions: None, max_output_tokens: 2048, reasoning: None,
}, 128, 256 * 1024, CancellationToken::new()).await?;
println!("{}", response.message.text());
# Ok(())
# }
```

生成控制全部来自 `ModelRequest`。Chat 默认使用 `max_completion_tokens`；对明确
使用旧字段的目标服务，将 `Config.max_tokens_field` 设为 `max_tokens`，不自动猜测或重试。
`supports_reasoning_effort` / Responses 的 `supports_reasoning` 默认关闭；确认目标模型
支持后显式开启，分别映射 `reasoning_effort` 和 `reasoning.effort`。Responses 请求
`reasoning.encrypted_content`，使用 `store:false`，不依赖隐藏的 response ID 链。
Chat/Responses 的输出 token 上限按服务语义包含模型的推理 token；Chat 旧版
`max_tokens` 的计数与适用模型需由具体兼容服务确认。

Anthropic 的 `version` 默认 `2023-06-01`。对确认支持的模型，可分别开启
`supports_adaptive_thinking`（Low/Medium/High → adaptive thinking + effort）或
`supports_thinking_budget`（BudgetTokens → enabled thinking + budget_tokens）。预算至少
1024 且小于本次 max_output_tokens；推理预算与强度不做数值换算。手动预算只适用于
支持它的模型；模型能力由宿主核实后配置。服务的 max_tokens 计入 thinking 输出。
不支持的请求设置在发网络请求前失败。工具 schema 原样映射；OpenAI function 声明
显式使用 `strict:false`，Anthropic 保持默认非 strict 语义。

工具头可能被拆片，Chat 在 finish_reason 出现前有界保留头与参数片段，随后发布
完整头和保持交错顺序的参数增量。文本仍即时输出。Responses/Anthropic 按类型化
事件发布块增量。整个响应分别要求 `[DONE]`、response.completed/incomplete 或
message_stop；单独的块结束或 EOF 不构成成功。

每条完整响应复用 `AssistantMessage` 进入历史。Responses 和 Anthropic 的续接材料
带稳定来源标识，覆盖协议、完整端点、模型、账号/租户隔离标识与相关配置。
同配置重建适配器可复用；换服务、模型、隔离范围或解释配置时须重新验证。
回传 native items/blocks 时校验公开投影并避免重复编码；公开摘要不能替代加密材料或签名。
TaskContext/TaskUpdate 只编码为 user 数据。Core 不解释续接内容，不把摘要或载荷播报。

文本本地上限（不是供应商上限）：编码前请求载荷合计 1 MiB、最多 4096 条消息及 4096 个历史内容块；请求线协议 JSON 1 MiB；单 HTTP 数据块 1 MiB；
SSE 单行/单事件 data 256 KiB；整次 HTTP 流 16 MiB；Responses/Anthropic 原生保留材料
1 MiB；Chat 工具参数缓冲 1 MiB / 4096 个非空片段。协议 item/block 索引小于 256。
Core 另以调用方提供的合计上下文字节预算限制消息、声明、指令、公开输出和续接材料，
最多 256 个公开内容块、32 个工具调用。所有流按消费速度读取，无后台生产任务。
首个错误即释放流，截断结果不执行工具、不自动续写或提交成功 assistant 历史。
当前语音流程仍等待完整模型响应后执行工具。所有普通正文只交付转录；
主模型通过 `speak({"text":"..."})` 显式请求异步播报，最终回复也不会自动 TTS。
Core 维护一条执行中请求和最多五条等待请求，满时丢弃最早的等待项；
宿主确认整段播放完成后，才开始合成下一条。供应商 TTS 端口不承担队列调度。

## 启动时组装

以下示例将阿里 STT 与火山 TTS 独立注入 Core 的能力接口。具体服务模型、资源权限和音色必须由宿主按账号选择；同协议下更换模型配置后重建适配器和会话。

```rust,no_run
# #[cfg(all(feature = "aliyun", feature = "volcengine"))]
# fn main() -> furica_core::Result<()> {
use std::sync::Arc;
use furica_adapters::{aliyun, volcengine};
use furica_core::{stt::Stt, tts::Tts};

let stt: Arc<dyn Stt> = Arc::new(aliyun::AliyunStt::new(aliyun::SttConfig {
    api_key: std::env::var("DASHSCOPE_API_KEY").expect("DASHSCOPE_API_KEY"),
    endpoint: "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation".into(),
    model: "fun-asr-realtime".into(),
})?);

let tts: Arc<dyn Tts> = Arc::new(volcengine::VolcengineTts::new(volcengine::TtsConfig {
    api_key: std::env::var("VOLCENGINE_API_KEY").expect("VOLCENGINE_API_KEY"),
    endpoint: "https://openspeech.bytedance.com/api/v3/tts/unidirectional".into(),
    resource_id: "seed-tts-2.0".into(),
    model: "seed-tts-2.0-standard".into(),
    voice: std::env::var("VOLCENGINE_VOICE").expect("VOLCENGINE_VOICE"),
    sample_rate: 24_000,
})?);

// 将 stt、宿主选择的 TextModel、tts 传给 CascadedBackend::new。
// 两者均已可用作 Arc<dyn ...>，无须在 Core 中判断供应商。
# let _ = (stt, tts);
# Ok(())
# }
# #[cfg(not(all(feature = "aliyun", feature = "volcengine")))]
# fn main() {}
```

阿里 CosyVoice 配置使用 `aliyun::TtsConfig`，包含 `api_key`、完整 WebSocket `endpoint`、`model`、`voice`、`sample_rate`。公共端点为 `wss://dashscope.aliyuncs.com/api-ws/v1/inference`。百炼业务空间专属域名可通过 `endpoint` 配置；Fun-ASR 这条 HTTP 路径使用北京地域。

火山 STT 使用 `volcengine::SttConfig`，包含 `api_key`、`endpoint`、`resource_id`、`model`。端点为 `https://openspeech.bytedance.com/api/v3/auc/bigmodel/recognize/flash`，所实现协议使用 `volc.bigasr.auc_turbo` 资源与 `bigmodel` 请求模型。仅接入新控制台 `X-Api-Key` 鉴权；旧 AppID/AccessToken 鉴权未实现。

## 调用与资源契约

- STT 接收完整 `Utterance` 并返回最终 `String`。音频在内存封装，不写临时文件、不上传对象存储；按顺序检查帧、采样率与上限。确认静音或成功的空转写返回空字符串；缺少结果字段、服务错误不会被当作静音。
- TTS 只实现 Core 的 `synthesize_stream`。按服务实际产出交付非空单声道 PCM16 块；网络字节边界不要求对齐样本边界。服务控制消息可以没有音频，最终音频先于成功结束交付。
- 供应商的 JSON、鉴权和任务事件不进入 Core。Core 校验输出格式、在追加前检查累计时长，并在整段成功后交付当前播放链路。本次没有实现边合成边播放。
- 流直接持有 HTTP 响应或 WebSocket，按消费速度读取，不启动生产任务。取消在等待连接/响应或轮询流时生效；释放 future/流即释放适配器所持请求。HTTP 连接复用由客户端管理，不能保证远端立即停止计算或计费。
- HTTP 客户端保留系统代理配置，回环端点直连。端点要求 HTTPS/WSS，本机回环测试允许 HTTP/WS；不跟随 HTTP 重定向。不在错误中回显密钥、鉴权 URL 或供应商原始错误消息。
- 独立调用者应提供截止时间，例如用 `tokio::time::timeout` 包住打开及整个消费过程；Core 已有整轮推理超时。首期不自动重试，以免重复合成或重复计费。

当前资源限制：STT 最长 120 秒、最多 4096 帧，采样率 8–48 kHz，仍受所选服务支持范围约束；STT 响应最大 1 MiB；单条 TTS 消息和 JSON 缓冲最多 2 MiB。CosyVoice 文本最多 20,000 UTF-8 字节，火山 TTS 首期保守限制为 1024 UTF-8 字节，超限明确拒绝，不自动切句或截断。

TTS 优先向服务请求目标采样率的原生 PCM。CosyVoice 接受 8/16/22.05/24/44.1/48 kHz；火山接受 8/16/22.05/24/32/44.1/48 kHz。构造时拒绝其他采样率；本版不引入通用压缩音频解码或任意采样率重采样。宿主须使 TTS 输出率与 Core 配置一致，默认的 24 kHz 可直接使用。

## 协议依据与验证边界

- [OpenAI Chat Completions / Responses 迁移](https://developers.openai.com/api/docs/guides/migrate-to-responses)
- [OpenAI reasoning items 续接](https://developers.openai.com/api/docs/guides/reasoning#keeping-reasoning-items-in-context)
- [Anthropic 流式 Messages](https://platform.claude.com/docs/en/build-with-claude/streaming)
- [Anthropic 手动 thinking 预算及模型适用性](https://platform.claude.com/docs/en/build-with-claude/extended-thinking)

- [阿里非实时 Fun-ASR HTTP](https://help.aliyun.com/zh/model-studio/non-real-time-speech-recognition-for-fun-asr-realtime)
- [CosyVoice 客户端事件](https://help.aliyun.com/en/model-studio/cosyvoice-client-events)、[服务端事件](https://help.aliyun.com/en/model-studio/cosyvoice-server-events)
- [火山极速 STT 详细协议](https://docs.volcengine.com/docs/DoubaoVoice/LargemodelrecordingfileLiterecognitionAPI?lang=zh)
- [火山单向 HTTP Chunked TTS](https://docs.volcengine.com/docs/DoubaoVoice/HTTPChunkedSSEUnidirectionalStreaming-V3?lang=zh)

火山 STT 详细文档支持内联 `audio.data`，简化文档仅列 URL。本实现按详细协议发送内联 WAV；目标账号的可用性须在真实凭证联调时验证，失败不会自动上传到对象存储。

离线协议测试使用本机回环 HTTP/WebSocket 服务；文本测试覆盖混合内容、交错参数、UTF-8 与多行 SSE、结束状态、续接、用量、资源边界、取消、释放和 feature 隔离，语音回归覆盖请求头、WAV 封装、任务顺序、拆包与多消息、PCM 跨消息半样本、最终消息音频、服务错误、异常断流、取消和释放。没有调用真实云端服务，不将这些测试作为账号权限、模型效果、音质或实际时延验收。

从仓库根目录运行：

```sh
cargo test --locked -p furica-core
cargo test --locked -p furica-adapters --all-features
cargo check --locked -p furica-adapters --no-default-features
cargo clippy --locked -p furica-core -p furica-adapters --all-features --all-targets -- -D warnings
cargo +1.88.0 check --locked -p furica-core -p furica-adapters --all-features
```

协议测试需要允许绑定本机回环端口；没有端口权限时明确失败，不静默跳过。


## ACP 执行适配器

启用 `acp` feature 后，从 `furica_adapters::acp::{AcpConfig, AcpExecutor}` 构造执行器，
注入 Core 的 `sessions::SessionManager`。`AcpConfig::agent_id` 是宿主配置引用，默认取
启动程序名称；宿主可覆盖为稳定的配置标识。协议、stdio、鉴权、超时与进程清理由此模块负责。

业务 Session、消息和执行状态由 Core 管理；数据库与配置文件读写由宿主实现。
模型调用工具仅准备 Run，宿主保存数据后调用 `launch` 才会启动 ACP 执行。
消息 chunk 与工具字段 patch 转换为 Core 执行事件；完整消息和工具最终记录可从
SessionManager 快照取得，摘要限长不影响完整记录。未提供的工具名称、参数或输出保持缺失。

`cargo test -p furica-adapters --features acp --test acp --locked` 运行脚本 Agent 协议测试。
真实 Agent 冒烟用例需要配置环境变量后显式启用；它默认忽略。
