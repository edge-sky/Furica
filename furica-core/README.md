# furica-core

Provider-independent session behavior, voice orchestration and capability contracts.
The host owns persistence, schema migrations, credentials, configuration loading,
device capture and playback. Core contains no database implementation or database
errors. ACP, speech and text protocol adapters live in `furica-adapters`.

## Business sessions and host data

`Session` is a long-lived work conversation with one main-model context and any
number of independent ACP contexts. Closing a `VoiceSession` does not replace the
business session or discard its history. `Actor.epoch` only invalidates stale async
work; the host-supplied `session_id` is the stable business identity.

| Object | Responsibility |
| --- | --- |
| `Session` | ID, name, Active/Archived lifecycle and creation/update times |
| `Message` | Session/context ownership, role, source, structured content, ordered sequence and completion state |
| `AcpSession` | Local/remote IDs, host agent-profile identity, workspace, connection state and timestamps |
| `Run` | A prepared execution, input-message reference, request deduplication and terminal outcome |
| `ToolCall` | Source call ID, input, output, structured content/locations and current state |
| `Question` | Permission/elicitation request, associated execution/tool, answer and deadline |

Import public models and use cases from `sessions`. `SessionData` contains the six
record collections. Pass host-loaded values to `SessionManager::new`; use
`create_session` or `add_session` for a new business session. Names initially use
`新会话`; the first nonblank main user message supplies a name of up to 48 characters.
`rename_session` accepts a host-selected name. Archived sessions remain queryable
and reject new messages/executions. Already launched work can finish while its ACP
connection is released.

Main messages have `acp_session_id = None`; agent messages select their ACP context.
Only main-context messages enter main-model history. Opaque model continuations are
preserved with their assistant message. ACP chunks are assembled into current
message records and tool patches update current calls; raw event histories are not
retained. Incomplete content is marked Interrupted and is excluded from model replay.
ACP capture covers the information actually reported by the agent; optional model,
name, argument and result fields are not invented.

## Main-model execution and host save acknowledgement

Only the main-model workflow prepares and controls ACP work. The host submits
user text/audio through `CoreHandle`; the model uses `prepare_run` or
`prepare_next_run` to request execution. ACP preparation, launch, answers,
cancellation and close are internal commands, not public host methods.

Prepared work waits until the host successfully saves its snapshot and calls
`acknowledge_saved(&snapshot)`. Core then schedules only model-prepared runs in
that snapshot, in preparation order. Work created while a save is in progress
waits for a subsequent save. Repeated acknowledgements never repeat accepted
execution; cancelled, completed or imported interrupted work is not restarted.

```rust,no_run
use furica_core::{Result, sessions::{SessionData, SessionManager}};

// Called inside the host's Tokio runtime, initially and after change notifications.
fn save_latest(
    sessions: &SessionManager,
    save: impl FnOnce(&SessionData) -> Result<()>,
) -> Result<()> {
    let saved = sessions.snapshot()?;
    save(&saved)?; // If saving fails, do not acknowledge.
    sessions.acknowledge_saved(&saved)
}
```

`changes()` provides a coalescing watch notification; `snapshot()` returns a complete
consistent value. Subscribe before taking the initial snapshot, process saves
serially, and reread the full snapshot after each notification. Pass the same
successfully saved snapshot to `acknowledge_saved`; do not read a fresh snapshot
between saving and acknowledging. Core checks prepared execution/input/configuration
against current memory but cannot verify that a database transaction committed.
Do not hold a watch borrow while querying/updating the manager. The host flushes a
final snapshot after shutdown and handles save failures and retries.

The host can observe ACP work through these read-only queries:

| Interface | Result |
| --- | --- |
| `acp_sessions(session_id)` | ACP contexts belonging to a business session |
| `status(session_id, acp_session_id)` | ACP context, its runs and pending questions |
| `run(run_id)` | One execution's state, summary, error and timestamps |
| `recent_runs_for_session(session_id, limit)` | Recent executions for a business session |
| `subscribe()` | Live execution, progress and question notifications |

The broadcast feed is for UI notifications and may lag; reread a snapshot after
`Lagged`. It is not a durable change log. Complete messages and tool results are in
`SessionData`; `Run.summary` is a bounded display summary.

Imported data is validated before use. Connections are marked Disconnected,
unfinished executions Interrupted and pending questions Cancelled. Orphaned main
tool exchanges receive an explicit unavailable-result record. No process is restored
and no prompt is replayed. The host owns any old database-format conversion.

## Runtime and configuration

`Core::start(config, backend, sessions.clone(), session_id)` and
`Core::start_gated(config, input, pipeline, sessions.clone(), session_id)` select an
existing active business session. Configuration values are supplied when creating
components; there is no configuration-file loading or live configuration mutation.
A custom `VoiceBackend::connect(sessions, session_id)` receives the runtime manager
and must record main messages through `append_message`, preserving roles, sources,
continuations and tool results. `CascadedBackend` implements this contract, including
STT provenance and completed model output when TTS subsequently fails.

The execution tools are `prepare_run`, `prepare_next_run`, `get_execution_status`,
`answer_question`, `cancel_run` and `close_acp_session`. ACP references use the
`acp_session_id` argument. Result/question announcements are scoped by the stable
business session; pending questions keep voice input active until answered or
expired. Explicit `end_conversation` cancels pending questions but keeps business
history. Voice idle/mute closure keeps the business session usable. Shutdown cancels
execution and waits for executor-side cancellation/cleanup.

## Module responsibilities

- `sessions/domain/` owns session, message and execution rules.
- `sessions/application/` owns validated host data, canonical memory records,
  ACP record assembly and execution scheduling.
- `sessions/ports.rs` defines Executor, ExecutionInput and ExecutionEvent.
- `orchestration/` owns wake/verification, turns, tools, playback and cancellation.
- `stt`, `text_model`, `tts`, `input`, `playback` own independent capabilities.
- `furica-adapters::acp` implements the executor when feature `acp` is enabled.

## Independent calls

The free functions `stt::transcribe`, `text_model::generate` and `tts::synthesize`
apply Core validation around an injected adapter. They require no voice session,
playback device or task manager. Callers supply limits and own the overall deadline,
for example with `tokio::time::timeout`. Cancellation drops pending adapter calls;
Text and TTS cancellation also cover stream consumption. Dropping a call drops its locally
owned work, subject to the adapter's cancellation/drop contract.

```rust,no_run
use furica_core::{Result, text_model::{self, TextModel, ModelRequest, ModelMessage, ModelResponse}};
use tokio_util::sync::CancellationToken;

async fn answer(model: &dyn TextModel, cancel: CancellationToken) -> Result<ModelResponse> {
    text_model::generate(
        model,
        ModelRequest {
            messages: vec![ModelMessage::User("Summarize this text.".into())],
            tools: vec![],
            instructions: None,
            max_output_tokens: 4096,
            reasoning: None,
        },
        128,       // Message budget, including the response.
        256 * 1024, // Combined messages, tools, instructions, continuation and output bytes.
        cancel,
    ).await
}
```

Text adapters implement only `TextModel::generate_stream` and expose a stable,
credential-free `continuation_source`. `text_model::generate_stream` validates
provisional block events; `generate` consumes the same validation state and returns
its assembled `ModelResponse`. Stop and drop a stream at its first error or terminal
event. EOF alone is not successful completion.

`AssistantMessage` preserves ordered `ModelContent` (text, tools and optional
reasoning summaries) and opaque `ModelContinuation`. Append that same assistant
message to history, then append matching `ToolResult` messages before the next
request. A `ToolCalls` finish requests execution; only the fully validated batch is
executable. `Truncated` preserves complete displayable content but must not execute
tools, automatically continue, or commit a successful assistant history entry.
`Refused` is visible and can be spoken. Summaries and continuation payloads are never
ordinary speech or system instructions. No full JSON Schema validator is implied;
tool parameters must be JSON objects and names must match declared tools.

The byte budget now includes tool declarations, instructions and continuation data,
in addition to history and output. Requests that fitted the former message-only
budget may now fail explicitly. Core allows at most 256 content blocks, 32 tools per
batch, 256-byte call IDs and 128-byte tool names. Adapters impose independent wire
limits. `ModelUsage` fields are optional: absent means unknown. `Reasoning` expresses
qualitative effort or a distinct token budget; unsupported settings fail before I/O.

The cascaded workflow config supplies `instructions`, `max_output_tokens` (default
4096) and `reasoning` on every request. It stores whole assistant messages and waits
for complete tool results before continuing. All ordinary assistant text, including
complete/refused final replies, is display-only. Speech requires the model to call
`speak({"text":"..."})`; a session-bound internal `CoreTools` registry supplies
both the six ACP tools and `speak`. Each entry binds its declaration to its handler. Only a fully validated tool batch can enqueue speech.

The registry dispatches calls and returns a `ModelMessage::ToolResult` with the
original `call_id`, including argument and dispatch errors. The cascaded worker
records each result once and continues after the complete batch; it no longer
routes ACP calls through a separate voice-event round trip. Custom voice backends
retain `VoiceEvent::ToolCall` / `VoiceCommand::ToolResult`; the actor uses the same
ACP entries and the backend still owns its history. `execution_tools()` remains
available and derives declarations from those entries. This registry is internal
and provides no host registration or plugin API. ACP execution still waits for
the host's save acknowledgement; speech follows its own playback queue.

`speak` immediately returns `accepted: true` and a unique `request_id`. Model
continuation does not wait for synthesis or playback. Each voice connection owns
one active request (from synthesis through actual playback completion) and at most
five waiting requests. Requests execute FIFO. On overflow, the oldest waiting
request is discarded and its ID is returned as `dropped_request_id` on the new
call; the active request is unaffected. Acceptance does not promise delivery.
Blank, oversized or malformed arguments return a tool error before queue mutation.

The host must report actual progress with `CoreHandle::report_playback`; validated
progress is forwarded as `VoiceCommand::Playback`. The next synthesis starts only
after the whole current item has played. Its audio `item_id` equals `request_id`.
Hosts must also observe playback generation changes and discard stale chunks.
Interrupting, ending, muting or shutting down clears active and waiting speech;
reconnection does not replay the queue. Speech tools/results remain in history.

`inference_timeout` bounds each STT/model turn and, independently, each speech
synthesis. Synthesis failures retain the existing voice-error behavior: report an
error, close the voice connection and discard queued speech while preserving
already recorded messages and tool results. Protocol implementations are opt-in in
`furica-adapters`.

TTS adapters still implement only `Tts::synthesize_stream`. The Core helper
`tts::synthesize` consumes that stream once, enforces the audio limit and returns
only a complete successful result. The current conversation workflow waits for
that result before delivering audio; it does not play partial synthesis.

## Public imports and checks

Use `sessions::{Session, Message, AcpSession, Run, ToolCall, Question, SessionData,
SessionManager, SessionLimits}` for host data and queries, and
`orchestration::{Core, VoicePipeline, CascadedBackend,
CascadeConfig, VoiceBackend, VoiceSession, VoiceChannels, execution_tools}` for voice.
`sessions::{Executor, ExecutionInput, ExecutionEvent}` is the adapter implementation
port. Concrete ACP types are `furica_adapters::acp::{AcpConfig, AcpExecutor}`; the
host configures and injects them at startup. ACP control stays in the main-model workflow.
The former `tasks` module and SQLite interfaces have been removed.

Run `cargo test --workspace --all-features --locked` and
`cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`.
Protocol fixtures live in `furica-adapters/tests/fixtures`; the external-agent smoke
test remains opt-in. See [the session data guide](../docs/session-data-flow.md).
