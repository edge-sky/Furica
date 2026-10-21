# Furica Web

独立的本地 Web 宿主，适配 main 的 `SessionManager`、`SessionData` 和工具驱动语音编排。界面采用暖白、鼠尾草绿、陶土色和雾蓝色，提供历史对话、文字与语音输入、执行进展侧栏和设置面板。

## 启动

需要 Rust 1.88+。在仓库根目录运行：

```sh
cargo run --manifest-path furica-web-demo/Cargo.toml --locked -- --port 8765
```

打开 <http://127.0.0.1:8765>，点击左下角设置，填写标有“必填”的文本模型、TTS 和 ACP 字段后点击“保存设置”；STT 默认使用本地 SenseVoice，VAD 默认使用本地 Silero，无需填写路径或密钥。配置及 API Key 保存在 `runtime/settings.sqlite3`；`FURICA_DEMO_DATA_DIR` 可以覆盖数据目录。Unix 数据库权限为 `0600`。API Key 在本机 SQLite 中明文保存，接口只返回是否已配置，不回传已保存密钥；输入留空保留原值，勾选清除则删除密钥。

允许保存尚未填完的配置，页面显示剩余缺项；无效服务商、接口地址、参数或本地路径会拒绝保存，保留上次成功配置。保存成功后，下次启动对话会重新读取，无需重启宿主；进行中的对话继续使用启动时的配置。API Key 不再依赖环境变量。

首次使用从空配置开始，不读取或迁移旧 `config.json`、环境变量或旧数据库。`--config` 参数已移除；`config.example.json` 仅作为字段参考，服务端不加载它。历史对话仍使用现有 `runtime/sessions.json`，不迁移历史数据。

配置模块按职责组织在 `src/settings/`：`domain.rs` 复用现有配置和校验，`infrastructure.rs` 实现 SQLite 原子保存，`mod.rs` 将数据库操作放到 blocking worker，`presentation.rs` 返回去除密钥的页面投影；`web/settings/panel.js` 管理设置表单。

当前级联管线需要文本模型、TTS 和 ACP 配置。STT 服务商留空时使用 `furica-local` 的 SenseVoiceSmall INT8：模型与词表位于 `../furica-local/model/sensevoice-small.int8.onnx`、`sensevoice-tokens.txt`。默认 VAD 为同目录的 `silero_vad.onnx`，可通过“VAD 模型路径覆盖”指定其他文件；路径均相对 `furica-web-demo`。宿主已启用 `sensevoice` feature，模型加载在 blocking worker 中进行。只有主动选择云端 STT 时，才要求其服务地址、模型和 API Key；火山额外要求资源 ID。TTS 的资源 ID 同样仅在选择火山时必填。配置输入框没有预设示例或占位文本，已有真实配置仍正常显示；已保存密钥继续隐藏。声纹选项是流程测试替身，不能用于真实身份识别。文字模式可以发送消息并播放模型主动调用 `speak` 产生的语音，无需麦克风权限。

首次编译由 `ort-sys` 获取 ONNX Runtime 1.22。离线或受限网络下，可以从 [官方发布页](https://github.com/microsoft/onnxruntime/releases/tag/v1.22.0) 准备对应平台的库，再通过 `ORT_LIB_LOCATION=/path/to/onnxruntime/lib`、`ORT_PREFER_DYNAMIC_LINK=1`、`ORT_SKIP_DOWNLOAD=1` 构建；运行时还需让系统动态库搜索路径包含该目录。不要使用版本较旧的系统库代替 1.22。

## 与 core 的边界

- Web 直接复用 core 的六类会话记录，不维护另一套任务领域模型。
- `sessions/infrastructure.rs` 将完整 `SessionData` 原子写入 `runtime/sessions.json`。可以通过 `FURICA_DEMO_DATA_DIR` 指定存储目录。
- 保存器独立运行，先落盘并同步，再对同一份快照调用 `acknowledge_saved`。保存失败会重试，期间不启动尚未确认的执行；停止连接时保存最终状态。
- 主对话与 ACP 上下文分开展示。继续、取消、关闭和问题回答以明确的用户请求进入主对话，由模型调用 core 工具；浏览器不再直接调用 ACP 控制接口。
- 普通文本用于显示；需要发声时模型调用 `speak`。浏览器按实际播放进度回报 `played_ms`，并在 generation 变化时丢弃旧音频，使下一项合成可以按序推进。
- 结束语音对话会保留业务会话；停止连接和关闭页面会按 core 约定关闭执行运行时。尚有回应或执行进行时禁止切换会话，避免无意取消工作。
- 重启后恢复历史数据，未完成的工作按 core 的恢复规则标记为中断，不自动重放。旧 `tasks.sqlite` 保留原样；如需导入旧数据，宿主需要显式转换到 `SessionData`。

HTTP 与 WebSocket 仅绑定 loopback，保留 Host、同源检查和独占连接约束。设置接口 `GET /api/settings` 返回可编辑字段与缺项，`PUT /api/settings` 要求同源 Origin 和 JSON，限制为 64 KiB；密钥留空/缺省保留，JSON null 显式清除。没有修改根 workspace 或 core 的行为。

## 验证

```sh
cargo test --manifest-path furica-web-demo/Cargo.toml --locked
# 已准备仓库中的真实 SenseVoice 模型和 WAV 样本时：
cargo test --manifest-path furica-web-demo/Cargo.toml --locked no_stt_settings_uses_real_local_recognizer -- --ignored
cargo clippy --manifest-path furica-web-demo/Cargo.toml --locked --all-targets -- -D warnings
cargo fmt --manifest-path furica-web-demo/Cargo.toml --check
node furica-web-demo/web/conversation/audio.test.mjs
```

Rust 测试另覆盖 SQLite 重开、密钥保留与清除、无效设置不覆盖、损坏数据库保护和权限；会话测试覆盖原子保存、损坏数据保护、保存失败不执行、成功保存后执行、重复确认不重复执行和重启不重放；浏览器音频测试覆盖累计播放完成位置与中断后丢弃旧 generation。

可选端到端检查使用本地真实 ASR 模型以及模拟文本模型和 TTS 服务，不访问真实提供商、不启动真实 Agent：

```sh
cargo build --manifest-path furica-web-demo/Cargo.toml --locked
# Python 环境需安装 playwright，并准备 Chromium。
python furica-web-demo/tests/browser_smoke.py
```

该检查还验证无需云端 STT 配置、默认本地 VAD、必填标记随服务商切换、输入框无预设文本、实际设置表单保存、失败保留输入、密钥清除与不回显、宿主重启恢复、下一次启动应用新配置、移动端宽度及 HTTP 来源/请求大小检查；同时验证真实 WebSocket、core、适配器、两个排队的 `speak`、实际浏览器播放回报、快照保存和刷新恢复。`FURICA_WEB_BIN` 可指定宿主二进制，`FURICA_CHROMIUM` 可指定 Chromium；也可使用 Playwright 自带的浏览器。真实麦克风、提供商凭据和外部 ACP Agent 需要使用者的本地配置与设备验证。
