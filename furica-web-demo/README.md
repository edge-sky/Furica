# Furica Web

独立的本地 Web 宿主，适配 main 的 `SessionManager`、`SessionData` 和工具驱动语音编排。界面采用暖白、鼠尾草绿、陶土色和雾蓝色，提供历史对话、文字与语音输入、执行进展侧栏和设置面板。

## 启动

需要 Rust 1.88+。在仓库根目录运行：

```sh
cp furica-web-demo/config.example.json furica-web-demo/config.json
cargo run --manifest-path furica-web-demo/Cargo.toml --locked -- --port 8765
```

打开 <http://127.0.0.1:8765>。未填写配置时也可以浏览界面、已有会话和配置缺项；不会模拟模型回复。配置中的 `api_key_env` 填写环境变量名称，凭据通过宿主环境提供。`config.json` 不发送给浏览器，也不纳入 Git。

当前级联管线同时需要 STT、文本模型、TTS 和 ACP 配置；语音模式额外需要本地 Silero VAD 模型。VAD 使用 `furica-local`，模型文件由宿主提供。声纹选项是流程测试替身，不能用于真实身份识别。文字模式可以发送消息并播放模型主动调用 `speak` 产生的语音，无需麦克风权限。

首次编译由 `ort-sys` 获取 ONNX Runtime 1.22。离线或受限网络下，可以从 [官方发布页](https://github.com/microsoft/onnxruntime/releases/tag/v1.22.0) 准备对应平台的库，再通过 `ORT_LIB_LOCATION=/path/to/onnxruntime/lib`、`ORT_PREFER_DYNAMIC_LINK=1`、`ORT_SKIP_DOWNLOAD=1` 构建；运行时还需让系统动态库搜索路径包含该目录。不要使用版本较旧的系统库代替 1.22。

## 与 core 的边界

- Web 直接复用 core 的六类会话记录，不维护另一套任务领域模型。
- `sessions/infrastructure.rs` 将完整 `SessionData` 原子写入 `runtime/sessions.json`。可以通过 `FURICA_DEMO_DATA_DIR` 指定存储目录。
- 保存器独立运行，先落盘并同步，再对同一份快照调用 `acknowledge_saved`。保存失败会重试，期间不启动尚未确认的执行；停止连接时保存最终状态。
- 主对话与 ACP 上下文分开展示。继续、取消、关闭和问题回答以明确的用户请求进入主对话，由模型调用 core 工具；浏览器不再直接调用 ACP 控制接口。
- 普通文本用于显示；需要发声时模型调用 `speak`。浏览器按实际播放进度回报 `played_ms`，并在 generation 变化时丢弃旧音频，使下一项合成可以按序推进。
- 结束语音对话会保留业务会话；停止连接和关闭页面会按 core 约定关闭执行运行时。尚有回应或执行进行时禁止切换会话，避免无意取消工作。
- 重启后恢复历史数据，未完成的工作按 core 的恢复规则标记为中断，不自动重放。旧 `tasks.sqlite` 保留原样；如需导入旧数据，宿主需要显式转换到 `SessionData`。

HTTP 与 WebSocket 仅绑定 loopback，保留 Host、同源检查和独占连接约束。没有修改根 workspace 或 core 的行为。

## 验证

```sh
cargo test --manifest-path furica-web-demo/Cargo.toml --locked
cargo clippy --manifest-path furica-web-demo/Cargo.toml --locked --all-targets -- -D warnings
cargo fmt --manifest-path furica-web-demo/Cargo.toml --check
node furica-web-demo/web/conversation/audio.test.mjs
```

Rust 测试覆盖原子保存、损坏数据保护、保存失败不执行、成功保存后执行、重复确认不重复执行和重启不重放；浏览器音频测试覆盖累计播放完成位置与中断后丢弃旧 generation。

可选端到端检查使用本地模拟模型和 TTS 服务，不访问真实提供商、不启动真实 Agent：

```sh
cargo build --manifest-path furica-web-demo/Cargo.toml --locked
# Python 环境需安装 playwright，并准备 Chromium。
python furica-web-demo/tests/browser_smoke.py
```

该检查验证真实 WebSocket、core、适配器、两个排队的 `speak`、实际浏览器播放回报、快照保存和刷新恢复。`FURICA_WEB_BIN` 可指定宿主二进制，`FURICA_CHROMIUM` 可指定 Chromium；也可使用 Playwright 自带的浏览器。真实麦克风、提供商凭据和外部 ACP Agent 需要使用者的本地配置与设备验证。
