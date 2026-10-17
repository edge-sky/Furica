//! Conversation state and decisions, without channels, timers or provider SDKs.

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreState {
    Standby,
    Verifying,
    Connecting,
    Active,
    Muted,
    Stopped,
}

impl CoreState {
    /// Ending or failing a muted conversation must preserve the mute state.
    pub(super) fn after_end(self) -> Self {
        if self == Self::Muted {
            Self::Muted
        } else {
            Self::Standby
        }
    }

    /// True means a new connection is needed; false means already activated.
    pub(super) fn validate_manual_wake(self, gated: bool) -> Result<bool> {
        if gated {
            return Err(Error::State(
                "gated input requires speaker verification".into(),
            ));
        }
        match self {
            Self::Active | Self::Connecting | Self::Verifying => Ok(false),
            Self::Muted | Self::Stopped => {
                Err(Error::State("microphone is muted or core stopped".into()))
            }
            Self::Standby => Ok(true),
        }
    }
}

pub(super) fn can_announce_result(
    user_speaking: bool,
    responding: bool,
    session_active: bool,
    playback_finished: bool,
) -> bool {
    !user_speaking && !responding && session_active && playback_finished
}
