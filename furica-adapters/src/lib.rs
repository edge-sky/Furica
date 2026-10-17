//! Replaceable implementations of Furica's capability contracts.
//!
//! Provider modules are opt-in; the default build does not include cloud clients
//! or the local ONNX engine. Hosts construct adapters and inject them into Core.
#![doc = include_str!("../README.md")]

#[cfg(feature = "aliyun")]
pub mod aliyun;
#[cfg(any(feature = "aliyun", feature = "volcengine"))]
mod cloud;
#[cfg(feature = "volcengine")]
pub mod volcengine;
