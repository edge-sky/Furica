//! Conversation bounded context: state rules and application orchestration.
//! Public entry points are exported here; implementation layers remain private.

mod application;
mod domain;
mod ports;

pub use application::{
    CascadeConfig, CascadedBackend, Core, CoreEvent, CoreHandle, CoreOutput, VoiceChannels,
    VoicePipeline, VoiceSession, execution_tools,
};
pub use domain::CoreState;
pub use ports::{VoiceBackend, VoiceCommand, VoiceEvent};
