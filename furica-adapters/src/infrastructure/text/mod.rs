//! Private mechanisms shared by the text protocol adapters.
mod codec;
mod transport;
pub(crate) use codec::*;
pub(crate) use transport::{Http, Sse};
