# Furica Web

`furica-host` 的浏览器外壳。Core 负责业务规则和工具执行，Host 负责配置校验、适配器装配、会话持久化、运行生命周期及语音通道；Web 负责页面、麦克风、播放和实际播放进度反馈。

Web 进程提供静态页面和到指定 Host 的流式同源转发，不再直接依赖 core、local 或 adapters，不再保存另一份配置或会话数据。Rust 文件仅保留启动入口与 `infrastructure/web.rs` 传输实现；浏览器按 conversation、sessions、settings 职责组织，不新增业务模型。

## 启动

需要 Rust 1.88+。首次启动可先使用最小配置。在仓库根目录执行以下命令，创建初始化文件；仅在文件尚不存在时执行：

```sh
mkdir -p runtime/host
python3 - <<'PY'
import json
from pathlib import Path
root = Path.cwd()
config = json.loads((root / 'furica-host/config.minimal.example.json').read_text())
config['workspace'] = str(root)
with (root / 'runtime/host.initial.json').open('x') as file:
    json.dump(config, file, ensure_ascii=False, indent=2)
    file.write('\n')
PY
```

初始化 Host，无需设置凭证环境变量：

```sh
cargo run -p furica-host -- "$PWD/runtime/host" --initialize "$PWD/runtime/host.initial.json"
```

最小配置只设置 HTTP 和 workspace，所有推理能力均关闭。需要会话模型时，参考 [Host 文档](../furica-host/README.md) 与 `furica-host/config.example.json`，并将 `furica-host/system-prompt.toml` 复制到 Host 数据目录。后续启动去掉 `--initialize`，由 Host 从 SQLite 读取配置：

```sh
cargo run -p furica-host -- "$PWD/runtime/host"
```

`--initialize` 指定的文件必须真实存在。当前 Host 会将该文件不存在、不可读或 JSON 无效等情况统一报告为 `invalid_configuration`。

Host 启动后，在另一个终端启动 Web：

```sh
cargo run --manifest-path furica-web-demo/Cargo.toml --locked -- \
  --port 8766 --host http://127.0.0.1:8765
```

打开 <http://127.0.0.1:8766>。Web 默认监听 `127.0.0.1:8766`、连接 `http://127.0.0.1:8765`；`--token-file PATH` 可指定 Host 的 `access-token` 文件，默认读取 `runtime/host/access-token`。Host 首次初始化省略 `http.token` 时会生成随机令牌并存入数据库；每次启动将当前值原子同步到该文件（Unix 权限 0600）。Host URL 只接受 HTTP 回环 IP 地址，不接受路径、URL 凭证、查询参数或重定向。Web 与 Host 分别运行；停止 Web 不会停止 Host 的任务。

浏览器访问 `/api/...`，Web 去掉 `/api` 前缀后转发到 Host。Bearer 令牌仅由服务端注入，不发给浏览器，不出现在 URL 中，也不保存在浏览器存储中。转发不携带浏览器 Cookie 或 Authorization，保留语音通道需要的 `X-Voice-Token`。浏览器写请求必须具有与 Web 监听地址匹配的 Origin；Web 校验 Host 和 Origin 后，向 Host 发起无 Origin 的本机令牌请求，因此无需为 Web 额外配置 Host CORS。

## 设置与数据职责

设置面板按键值逐项编辑现有 `HostConfig`：每项显示中文名称、点分配置键与对应输入控件，按文本模型、STT、TTS、VAD、ACP、工作目录与访问、容量与超时分组。协议与服务商使用下拉框，布尔值使用复选框，数值使用整数输入，启动参数与允许来源逐项添加/移除，无需编辑 JSON。可选能力可以关闭，协议切换只提交适用字段；可选参数留空时使用 Host 默认值。页面使用现有 Host `GET/PUT /configuration`，由 Host 校验并事务保存。失败保留表单输入，成功明确提示重启 Host 后生效。修改 Host 访问令牌后，先重启 Host，再重启 Web 重新读取令牌文件；修改监听地址时同步更新 Web 启动参数。

- `model`、`acp`、`speech.stt`、`speech.tts`、`speech.vad_model` 可分别省略或设为 `null`；没有会话模型仍可查看历史。
- 文字对话只需要会话模型，不要求 ACP、STT 或 TTS。Host 在未开启语音通道时不提供 `speak` 工具。
- 持续语音需要会话模型、VAD 和 STT，TTS 可选。没有隐式的本地语音配置；本地模型路径和 workspace 使用绝对路径，文件由使用者准备。
- 服务商密钥直接填写 `api_key`，Host 访问令牌直接使用 `http.token`，均以明文保存在本机 SQLite；数据目录权限为 0700、数据库权限为 0600。不再支持 `api_key_env`、`http.token_env` 或凭证环境变量回退。
- 密码输入框不回填密钥原文；空值保留、输入新值替换，API Key 可显式清除。Host 访问令牌支持替换。查询只返回 `api_key_configured` / `token_configured` 状态；重开设置或保存成功后输入框为空。协议或服务商改变时需重新填写密钥。
- 缺少 API Key 的模型/云端语音能力保持未启用，仍可进入设置补齐后重启 Host。启用会话模型还需要 Host 数据目录中的 `system-prompt.toml`，修改后重启生效。
- 打开现有版本 4 数据库时，Host 在事务中移除旧环境变量引用；旧 `http.token_env` 会被一次性生成并持久化的新令牌替换，不读取原环境变量。现有会话与其他配置保留，服务商密钥需在设置中直接补填。
- 配置及会话记录由 Host 的 `configuration.sqlite3` 管理。Core 交付增量消息后，Host 先提交事务，再调用 `acknowledge_messages`；Web 不参与持久化确认。
- Web 不再读取旧 `settings.sqlite3`、`sessions.json`、`config.json` 或 `FURICA_DEMO_DATA_DIR`，不自动迁移旧 demo 数据。Host 也不导入旧 demo 快照；已有文件保留原样。

本次接入依据历史提交 `9de3eff` / `f0b7049`（Host 与 facade）、`24a8ca9`（数据职责更新）、`171278c`（提示词注入）、`98f10d6`（ACP 连接职责）、`cb6b82e`（配置校验职责），与当前 Host 的公开接口保持一致。

## 会话与事件

历史列表和选中会话的 messages、acp-sessions、runs、tool-calls、questions 使用 Host 分页查询，每页最多 200 条，按需继续读取后续页；不再传输所有会话的完整快照。主对话使用 Host 消息投影，不接收模型原始 continuation。ACP 内容与主对话分开展示，继续、取消、关闭与回答问题仍通过文字请求交由 core 工具执行。

事件使用 fetch SSE，支持 HTTP 分块和跨块 UTF-8；收到状态变化、`sessions_changed` 或 `resync` 后重新查询。SSE 断线后停止本页语音，显示重连入口；重连查询最新状态，不重放旧音频。会话切换遵守 Host 的任务/问题/语音占用限制。

## 语音

浏览器音频上下文以单声道 16 kHz 采集和播放。上传为 PCM16 小端，100 ms 一批（1600 样本），sequence 从 Host 返回的 `next_sequence` 开始；一次仅一个上传请求在途。429/网络失败只重试原批次，并遵守 Host 返回的次数和总时间预算；本地积压上限 5 批，超限结束通道。心跳独立于音频上传。

下载按 Host 的 `u32le header_length + JSON header + PCM` 协议解析，不把 HTTP 分块当作音频边界。SSE 和健康查询同步 playback generation，过期音频丢弃；只在浏览器实际播放后提交累计 `played_ms`。反馈串行发送并合并尚未发送的同项进度。

暂停聆听只静音本机麦克风；结束/关闭语音调用 Host 的通道关闭接口，不关闭业务会话或全局任务。页面关闭尽力释放通道，意外退出由 Host 心跳超时回收。设备授权、AEC 和播放由浏览器负责；旧测试声纹控件已移除。

## 验证

```sh
cargo test --manifest-path furica-web-demo/Cargo.toml --locked --offline
cargo clippy --manifest-path furica-web-demo/Cargo.toml --locked --offline --all-targets -- -D warnings
cargo fmt --manifest-path furica-web-demo/Cargo.toml --check
npm test --prefix furica-web-demo/web
```

端到端脚本需要 Node.js、Playwright 包、Chromium 或 Chrome，以及仓库的真实 Silero VAD 模型。默认使用 macOS Google Chrome；`FURICA_CHROMIUM` 可覆盖浏览器可执行文件。Playwright 可通过本机 Node 模块或 `NODE_PATH` 提供。

```sh
cargo build -p furica-host --offline
cargo build --manifest-path furica-web-demo/Cargo.toml --locked --offline
node furica-web-demo/tests/browser_smoke.mjs
```

`FURICA_HOST_BIN` / `FURICA_WEB_BIN` 可覆盖二进制路径。脚本在临时目录启动真实 Host、Web 和 Chromium，以本机模拟服务替代模型/STT/TTS，不使用供应商凭证或外部 ACP Agent。覆盖 201 条历史的分页、令牌隔离、来源/正文限制、数据库密钥隐藏/保留/替换/清除、无凭证环境变量启动、令牌文件与轮换、键值表单回填/保存、可选能力与协议切换、数组/布尔/整数参数、配置校验与重启生效、纯文字模式、PCM 上传与心跳、两个排队 speak 的实际播放回报、关闭语音后继续对话、刷新恢复和移动端宽度。真实设备与供应商服务仍需在使用环境验证。
