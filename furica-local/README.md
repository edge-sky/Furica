# furica-local

Furica 本地语音能力实现，原包名为 `furica-vad`，现使用 `furica-local` / `furica_local`。包含 Silero VAD 和可选的 SenseVoiceSmall INT8 整句 STT。

默认只启用 VAD，不依赖 Core；`sensevoice` feature 增加 Core `Stt` 端口实现。模型与词表均从本地加载，运行时不下载。云端 ASR 留在 `furica-adapters`，Core 继续拥有音频、STT 契约和会话编排。

## SenseVoiceSmall 整句识别

```toml
furica-local = { path = "../furica-local", features = ["sensevoice"] }
```

唯一新增公开类型为 `asr::SenseVoiceStt`。构造器 `from_files(model_path, tokens_path)` 同步加载并预热模型；宿主在开始接收音频前调用。自动识别语种，启用标点和数字规范化，CPU 使用 2 个 intra-op 线程。

```rust,no_run
# #[cfg(feature = "sensevoice")]
# async fn example(utterance: furica_core::stt::Utterance) -> furica_core::Result<()> {
use furica_core::stt::Stt;
use furica_local::asr::SenseVoiceStt;
use tokio_util::sync::CancellationToken;

let model = SenseVoiceStt::from_files(
    "model/sensevoice-small.int8.onnx",
    "model/sensevoice-tokens.txt",
)?;
let text = model.transcribe(utterance, CancellationToken::new()).await?;
println!("{text}");
# Ok(())
# }
```

输入复用 Core `Utterance`：16 kHz 单声道 PCM16，总长 25 ms–30 s，最多 4096 帧，每帧非空且不超过 200 ms，帧序号严格递增。超长、格式不符及非法帧明确报错，不自动截断、补零或重采样。输出为最终 `String`，去除语言、情绪和事件等标签。独立 ASR 调用不重复运行 VAD；宿主应只提交确认的语音段。真实模型对纯静音可能产生文字，不能将 ASR 当作静音检测器。

推理和特征提取在 Tokio blocking worker 上执行，需要在 Tokio runtime 内调用。一个实例及其克隆共享一个推理槽；重叠调用返回 `Busy`，不建立无界队列。取消或丢弃 future 会通知特征提取/解码停止并请求 ONNX 终止推理；返回取消不等于原生计算已经结束，推理槽保留到 worker 实际退出。调用方负责总体超时。

复用与 VAD 相同的 `ort` / ONNX Runtime，使用 `kaldi-fbank` 提取 80 维 Hamming FBank，执行 7 帧窗口 / 6 帧步长的 LFR、模型元数据 CMVN 和 CTC 贪心解码。没有第二套 ASR 引擎或 Python 运行时依赖。加载时检查张量、元数据、词表与预热输出；这验证契约兼容性，不证明模型身份，模型哈希见 [模型清单](model/README.md)。

从仓库根目录识别本地 WAV：

```sh
cargo run --release --locked -p furica-local --features sensevoice \
  --example transcribe -- /path/to/16khz-mono-pcm16.wav
```

macOS 持续麦克风输入，句末输出 `Transcript: ...`：

```sh
cargo run --release --locked -p furica-local --features sensevoice \
  --example microphone -- --transcribe
```

该示例沿用已有采集、下混、重采样和 Silero VAD。ASR 使用独立线程，最多保留一个待识别语音段；队列满或识别失败明确停止。保留 VAD 开始确认所需的 64 ms 音频，排除尾部静音；正常停止补完已确认的语音段并等待识别完成。超过 30 秒的语音报错，第一版不做长句拆分。麦克风回调不执行 ASR。

## 文件组织

`vad/domain/` 保存原有边界触发规则，`vad/infrastructure/` 保存 Silero ONNX 实现；`asr/infrastructure/` 保存 SenseVoice 适配器、特征提取和解码。Core 现有领域类型和端口直接复用，不新增通用模型基类、转录 DTO 或平行的能力接口。VAD 原有类型仍从 crate 根导出，也可通过 `vad` 模块访问。

## Silero VAD

输入为 16 kHz、单声道 PCM16，每次恰好 512 样本（32 ms）；输出每帧原始语音概率和可选的开始/结束事件。VAD 模型加载与逐帧推理均为同步阻塞调用，由宿主安排工作线程。

```no_run
use furica_local::{FRAME_SAMPLES, SileroVad, VadConfig};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut vad = SileroVad::from_file("silero_vad.onnx", VadConfig::default())?;
// 按采集顺序重复调用，帧之间保留同一个实例。
let frame = [0_i16; FRAME_SAMPLES];
let output = vad.process(&frame)?;
println!("{} {:?}", output.speech_probability, output.event);
// 停止输入时显式结束当前语音段，并开始新的时间轴。
let final_event = vad.finish()?;
println!("{final_event:?}");
# Ok(())
# }
```

## 事件与状态

默认开始阈值为 `0.5`，连续语音至少 `64 ms`；结束阈值为 `0.35`，连续静音至少 `300 ms`，按完整帧向上取整为 `320 ms`。这些是待设备录音评估的工程初值。阈值必须有限且满足 `0 < silence_threshold < speech_threshold < 1`，两个时长必须大于零且可以表示为 `u64` 样本数。

`SpeechStarted.start_sample` 回溯至连续候选语音的第一帧。`SpeechEnded.end_sample` 为连续尾部静音的第一帧起点，使用排他边界。位置以最近一次 `reset` 或成功 `finish` 为零点，单位为 16 kHz 样本。外部必须保留覆盖开始确认窗口的 PCM；库只保留固定长度的模型上下文和状态。

`finish` 在已确认开始时补发结束事件，有尾部静音则排除该静音，否则使用已处理输入的末尾；成功后重置。未确认的短语音不产生事件。`reset` 丢弃旧流，不生成事件。音频丢帧或重启采集后应重置。

错误帧长度在推理前拒绝，不改变状态。推理失败或输出非法后，`process`、`finish` 返回 `NeedsReset`，必须显式 `reset`。采样率、声道数、AEC、分帧与不足一帧的尾部处理由外部负责；库不会自动补零或混合音频。

## 固定模型

验证使用官方 **Silero v6.2.1** 的流式 ONNX 模型：

- [模型来源](https://github.com/snakers4/silero-vad/blob/v6.2.1/src/silero_vad/data/silero_vad.onnx)
- SHA-256：`1a153a22f4509e292a94e67d6f9b85e8deb25b4988682b7e174c65279d8788e3`
- 大小：2,327,524 字节。
- [官方状态维护实现](https://github.com/snakers4/silero-vad/blob/v6.2.1/src/silero_vad/utils_vad.py)
- [模型 MIT 许可](https://github.com/snakers4/silero-vad/blob/v6.2.1/LICENSE)

宿主负责校验和分发模型。加载时检查输入输出契约并执行一次静音推理后重置；这验证契约兼容性，不证明模型身份。支持 `input`、`state`、`sr` 输入和 `output`、`stateN` 输出，16 kHz 分支使用 64 样本上下文及 `[2, 1, 128]` 循环状态，不承诺其他模型契约兼容。

## 构建与验证

Rust 最低版本为 1.88。锁定 `ort = 2.0.0-rc.10`（ONNX Runtime 1.22 绑定）；首次构建时由 `ort-sys` 获取并校验 CPU 原生库，运行时无 Python 依赖。离线构建需提前缓存依赖与原生库，或依照 [ort-sys 构建说明](https://github.com/pykeio/ort/blob/v2.0.0-rc.10/ort-sys/build.rs) 通过 `ORT_LIB_LOCATION` 提供原生库。

本次 macOS ARM64 构建使用静态 ONNX Runtime：`ort-sys` 链接日志为 `static=onnxruntime`，示例的 `otool -L` 没有独立 ONNX Runtime 动态库。分发时另附本地模型及其 MIT 许可，并保留 ONNX Runtime 的 [许可](https://github.com/microsoft/onnxruntime/blob/v1.22.0/LICENSE) 与 [第三方声明](https://github.com/microsoft/onnxruntime/blob/v1.22.0/ThirdPartyNotices.txt)。其他目标平台的链接和打包未验证。

```sh
cargo test --locked -p furica-local
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

真实模型测试默认标记为忽略，须显式提供上述固定模型；测试会校验 SHA-256，模型缺失或不匹配直接失败，不自动下载或静默跳过：

```sh
SILERO_VAD_MODEL=/path/to/silero_vad.onnx \
  cargo test --locked -p furica-local --test real_model -- --ignored
```

对本地原始 PCM 文件逐帧运行：

```sh
cargo run --locked -p furica-local --example detect -- \
  /path/to/silero_vad.onnx /path/to/16khz-mono-s16le.pcm
```

示例只读取原始 PCM，不接受 WAV 文件头；长度不足一帧的尾部报错，是否补零由调用方决定。每行输出帧起点、概率与可选事件，输入结束后调用 `finish`。

## 持续麦克风输入（macOS 示例）

运行 `microphone` 示例，默认读取该 crate 的 `model/silero_vad.onnx`，不依赖当前工作目录：

```sh
cargo run --locked --example microphone \
  --manifest-path furica-local/Cargo.toml
```

持续监听默认麦克风，每次说话输出 `SpeechStarted`、`SpeechEnded`、采样点及秒数。语音段结束后继续监听。首次使用需要麦克风授权；被拒绝时在“系统设置 → 隐私与安全性 → 麦克风”检查当前终端或 IDE 的权限。用 `Ctrl+C` 停止；也处理 `SIGTERM`、`SIGHUP` 的正常停止请求。

可选覆盖模型路径，或启用每帧概率：

```sh
cargo run --locked -p furica-local --example microphone -- --probabilities
cargo run --locked -p furica-local --example microphone -- /path/to/silero_vad.onnx
cargo run --locked -p furica-local --example microphone -- --help
```

`--help` 在模型加载、设备查询和录音之前返回。示例支持 8–192 kHz、1–32 声道，以及 `f32`、`f64`、`i16`、`u16`、`i32`、`u32` 输入。启动时显示实际设备格式。采集回调下混至单声道，通过覆盖 250 ms 原生音频的有界队列交给工作线程；非 16 kHz 输入采用 Rubato 256 点 sinc 抗混叠滤波，固定输入块长为 `ceil(采样率 / 100)`（约 10 ms），保留连续状态后转换、凑齐 512 样本并调用 VAD。

回调中不做推理、重采样、打印、文件 I/O 或热路径分配。队列满、非法数据、设备故障或推理失败明确停止，不自动切换设备、恢复或静默丢帧后继续。普通库不依赖采集组件；`cpal 0.18.2`、`rubato 5.0.1`、`rtrb 0.3.5`、`ctrlc 3.5.2` 仅为 macOS 示例的开发依赖。Rubato 关闭默认 FFT 功能，仅使用所需的 sinc 重采样。

正常停止先暂停并释放设备流，再排空完整数据，调用 `finish` 并等待工作线程退出。不补零；未满重采样块、未满 VAD 帧的尾部及未冲刷的滤波延迟在退出时报告。事件秒数对应 VAD 输出音频时间轴，包含重采样启动延迟，不是硬件采集时刻。`--probabilities` 的控制台输出速度也可能影响队列处理。

示例不保存音频，不包含 AEC 或声纹授权。扬声器播放的人声也可能触发 VAD。正常运行会打开麦克风；以下离线测试使用合成数据和微型模型，不打开设备：

```sh
cargo test --locked -p furica-local --example microphone
```

测试覆盖回调分块不变性、采样转换与下混、抗混叠、尾部不补零、缓冲过载、故障停止、正常退出排空以及多轮语音持续监听。完整方案见 [持续输入方案](../docs/microphone-input-proposal.md)。

## 验证记录（2026-10-20）

- 本次工作区 `cargo test --locked --workspace --all-targets --all-features` 共 109 项非忽略测试通过。云端协议测试使用本机回环，不调用真实云服务；外部 ACP Agent 冒烟仍需另外配置。
- 单独启用的 2 项真实 SenseVoice 测试通过：固定模型及词表哈希、中英文输出与独立 Python ONNX / 原生 FBank 参考一致、重复调用无状态串扰、Silero 静音过滤和真实语音分段后识别。原有真实 Silero 测试也通过。
- 新增离线测试覆盖输入范围和帧顺序、词表/模型不匹配、FBank + LFR + CMVN 参考一致性、CTC 重复/空白/标签处理、取消与推理槽复用，以及麦克风示例的起始回溯、尾部静音排除、下一句和队列满。
- 工作区 Clippy（全部 targets/features，`-D warnings`）、8 项文档测试、VAD-only 构建通过。Rust 1.88 下 local 的全部 features/targets 编译通过。
- 本机 macOS ARM64 的一次 release 文件识别：约 5.6 秒中文样本，模型加载及预热约 0.38 秒，识别调用约 0.09 秒。此记录不是性能保证或识别准确率评测。
- 本轮没有打开真实麦克风；设备权限、噪声环境、中英混说和真实用户句末延迟仍需运行示例验收。

复现本地验证：

```sh
cargo test --locked -p furica-local --all-features --all-targets
cargo test --release --locked -p furica-local --features sensevoice \
  --test real_sensevoice -- --ignored
SILERO_VAD_MODEL="$PWD/furica-local/model/silero_vad.onnx" \
  cargo test --locked -p furica-local --test real_model -- --ignored
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.88.0 check --locked -p furica-local --all-features --all-targets
```

## VAD 历史验证记录（2026-10-15）

- 环境：macOS ARM64；常规检查使用 Rust 1.99.0。
- Rust 1.88.0 下执行 `cargo +1.88.0 test --locked -p furica-local --all-targets -- --include-ignored` 通过（同时设置 `SILERO_VAD_MODEL`），包括 12 项 VAD 测试、10 项麦克风链路离线测试和 1 项真实模型测试，验证最低 Rust 版本兼容性。
- 7 项触发状态机测试、5 项公开接口/模型错误测试通过；测试夹具是独立的微型 ONNX 图，不是 Silero 模型。夹具与参考结果可通过 `tests/fixtures/generate.py` 复现。
- 固定 Silero v6.2.1 真实模型测试通过：24 帧静音/确定性波形的概率与 Python ONNX Runtime 1.19.2 独立参考结果误差不超过 `1e-6`；重置后逐帧输出完全一致；额外 50 帧静音无开始事件。
- 系统离线合成的英文语音，转换为 16 kHz PCM16 并加首尾静音后，通过真实模型及 `detect` 示例产生一对开始/结束事件：`start_sample = 16896`、`end_sample = 159232`，分别在帧起点 `17408`、`163840` 返回。
- `cargo test --locked --workspace --all-targets -- --include-ignored` 通过（设置上述模型路径），共 65 项测试，现有 Core 的 42 项测试无回归。`cargo test --locked --workspace --doc` 的两个文档示例均通过编译。
- `cargo clippy --locked --workspace --all-targets -- -D warnings` 通过。
- `cargo fmt --all --check`、`git diff --check` 通过。

以上验证本地推理契约与触发实现，不证明真实麦克风、中文短句、AEC 或复杂背景噪声下的准确率与端到端延迟。

本轮没有打开真实麦克风；设备格式、系统授权、实际断连和长时间录音稳定性仍需用户主动运行示例验证。离线测试中的设备故障与停止信号通过控制状态模拟，未声称完成真实设备验收。
