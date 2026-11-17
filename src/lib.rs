//! sudo-iroh-node — the part of iroh the SUDO-AI P2P tunnel uses, as a Node addon built with
//! napi-rs v3. Payload bytes go between JS and Rust as Buffer/Uint8Array with a single copy; they
//! are never spread into arrays of numbers.

mod conn;
mod endpoint;
mod stream;

pub use conn::*;
pub use endpoint::*;
pub use stream::*;

/// Turns an error of any kind into the `Error` a JS caller receives, keeping its message.
pub(crate) fn js_err(e: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(format!("{e}"))
}
