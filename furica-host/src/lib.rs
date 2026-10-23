//! HTTP facade, application use cases and SQLite persistence for Furica.
pub mod bootstrap;
pub mod configuration;
pub mod facade;
pub mod infrastructure;

pub use facade::application::{Components, Host};

pub use facade::error::{Error, Result};
