//! Stream halves. Each direction copies the payload exactly once: a write copies the caller's
//! Buffer/Uint8Array into refcounted bytes on the JS thread (so JS may reuse it immediately) and
//! hands those to QUIC; a read gathers QUIC's received chunks without copying and copies them into
//! one JS-owned Buffer on the JS thread.
//!
//! Each half serializes I/O behind a mutex. `stop` and `reset` first raise the half's cancel
//! signal, which makes a pending read or write give up its lock, so they never wait on a peer
//! that stays silent or withholds flow-control credit.

use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use iroh::endpoint::{self, ReadError, VarInt};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, watch};

use crate::js_err;

// Resolves once `stop` or `reset` has been requested for this half. The sender lives as long as
// the half itself, so a dropped sender means nobody can cancel and the wait never resolves.
async fn cancelled(mut rx: watch::Receiver<bool>) {
    if rx.wait_for(|requested| *requested).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Sending half of a bidirectional stream.
#[napi]
pub struct SendStream {
    inner: Arc<Mutex<endpoint::SendStream>>,
    cancel: watch::Sender<bool>,
}

impl SendStream {
    pub(crate) fn new(s: endpoint::SendStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(s)),
            cancel: watch::Sender::new(false),
        }
    }
}

#[napi]
impl SendStream {
    /// Writes all bytes; resolves when QUIC has accepted them (flow-control backpressure).
    #[napi(ts_return_type = "Promise<void>")]
    pub fn write<'env>(&self, env: &'env Env, data: &[u8]) -> Result<PromiseRaw<'env, ()>> {
        // The one copy: JS may reuse `data` as soon as this returns, and QUIC keeps the bytes
        // (refcounted, never copied again) until they are acknowledged.
        let bytes = Bytes::copy_from_slice(data);
        let inner = self.inner.clone();
        let cancel = self.cancel.subscribe();
        env.spawn_future(async move {
            // A reset abandons the write; otherwise wait for QUIC to accept every byte.
            tokio::select! {
                biased;
                () = cancelled(cancel) => Err(js_err("stream reset")),
                written = async { inner.lock().await.write_chunk(bytes).await } => {
                    written.map_err(js_err)
                }
            }
        })
    }

    /// Ends the send side cleanly (FIN).
    #[napi]
    pub async fn finish(&self) -> Result<()> {
        self.inner.lock().await.finish().map_err(js_err)
    }

    /// Abandons the send side with an application error code. A pending write rejects first, so
    /// the reset is not held up by a peer that withholds flow-control credit.
    #[napi]
    pub async fn reset(&self, code: u32) -> Result<()> {
        self.cancel.send_replace(true);
        self.inner
            .lock()
            .await
            .reset(VarInt::from_u32(code))
            .map_err(js_err)
    }
}

/// Largest `max` a single read honors; larger requests are clamped.
pub const MAX_READ: usize = 4 << 20;

// QUIC hands stream data out as one refcounted chunk per received STREAM frame, so a chunk is
// about one packet. Asking for `max / MIN_CHUNK` chunks lets a read reach `max` bytes.
const MIN_CHUNK: usize = 1024;

struct RecvInner {
    stream: endpoint::RecvStream,
    // Chunks QUIC already handed over that did not fit the previous read's `max`.
    carry: VecDeque<Bytes>,
    // Scratch slots for `read_many_chunks`, kept to avoid a per-read allocation of empties.
    slots: Vec<Bytes>,
}

impl RecvInner {
    // Takes up to `max` bytes from `carry`.
    fn take_carry(&mut self, max: usize) -> ReadBytes {
        let mut out = ReadBytes::default();
        while out.len < max {
            let Some(mut chunk) = self.carry.pop_front() else {
                break;
            };
            let room = max - out.len;
            if chunk.len() > room {
                let rest = chunk.split_off(room);
                self.carry.push_front(rest);
            }
            out.push(chunk);
        }
        out
    }

    // Waits for data, then gathers every chunk QUIC has ready (up to `max` bytes) under a single
    // connection lock. No byte is copied here. `None` is end of stream.
    async fn read(&mut self, max: usize) -> std::result::Result<Option<ReadBytes>, ReadError> {
        if self.carry.is_empty() {
            let want = max.div_ceil(MIN_CHUNK);
            if self.slots.len() < want {
                self.slots.resize(want, Bytes::new());
            }
            let Some(n) = self
                .stream
                .read_many_chunks(&mut self.slots[..want])
                .await?
            else {
                return Ok(None);
            };
            self.carry
                .extend(self.slots[..n].iter_mut().map(std::mem::take));
        }
        Ok(Some(self.take_carry(max)))
    }
}

/// Receiving half of a bidirectional stream.
#[napi]
pub struct RecvStream {
    inner: Mutex<RecvInner>,
    cancel: watch::Sender<bool>,
}

impl RecvStream {
    pub(crate) fn new(r: endpoint::RecvStream) -> Self {
        Self {
            inner: Mutex::new(RecvInner {
                stream: r,
                carry: VecDeque::new(),
                slots: Vec::new(),
            }),
            cancel: watch::Sender::new(false),
        }
    }
}

#[napi]
impl RecvStream {
    /// Reads whatever is available (at least 1 byte, at most `max`, which is clamped to 4 MiB),
    /// gathering every chunk QUIC has already received so one JS call covers many packets;
    /// resolves null at end of stream. Rejects if the peer reset the stream (message contains the
    /// reset code).
    #[napi(ts_return_type = "Promise<Buffer | null>")]
    pub async fn read(&self, max: u32) -> Result<Option<ReadBytes>> {
        if max == 0 {
            return Err(js_err("max must be > 0"));
        }
        let max = (max as usize).min(MAX_READ);
        // A stop abandons the read; otherwise wait for data, end of stream or a peer reset.
        tokio::select! {
            biased;
            () = cancelled(self.cancel.subscribe()) => Err(js_err("stream stopped")),
            read = async { self.inner.lock().await.read(max).await } => read.map_err(js_err),
        }
    }

    /// Asks the peer to stop sending, with an application error code. A pending read rejects
    /// first, so STOP_SENDING is not held up by a peer that stays silent.
    #[napi]
    pub async fn stop(&self, code: u32) -> Result<()> {
        self.cancel.send_replace(true);
        let mut inner = self.inner.lock().await;
        inner.carry.clear();
        inner.stream.stop(VarInt::from_u32(code)).map_err(js_err)
    }
}

/// Received chunks, still owned by QUIC's buffers. Converting them to JS allocates one JS-owned
/// Buffer of the exact size on the JS thread and copies the chunks into it: the only copy between
/// the decrypted packet and JS. The chunks are freed right there, so read memory never waits for a
/// GC.
#[derive(Default)]
pub struct ReadBytes {
    chunks: Vec<Bytes>,
    len: usize,
}

impl ReadBytes {
    fn push(&mut self, chunk: Bytes) {
        self.len += chunk.len();
        self.chunks.push(chunk);
    }
}

impl ToNapiValue for ReadBytes {
    unsafe fn to_napi_value(env: sys::napi_env, v: Self) -> Result<sys::napi_value> {
        let mut data = std::ptr::null_mut();
        let mut buffer = std::ptr::null_mut();
        check_status!(
            unsafe { sys::napi_create_buffer(env, v.len, &mut data, &mut buffer) },
            "failed to allocate a {} byte Buffer",
            v.len
        )?;
        let mut at = data.cast::<u8>();
        for chunk in &v.chunks {
            // SAFETY: `data` points at `v.len` writable bytes owned by the new Buffer, and the
            // chunk lengths sum to `v.len`.
            unsafe {
                std::ptr::copy_nonoverlapping(chunk.as_ptr(), at, chunk.len());
                at = at.add(chunk.len());
            }
        }
        Ok(buffer)
    }
}

impl TypeName for ReadBytes {
    fn type_name() -> &'static str {
        "Buffer"
    }
    fn value_type() -> ValueType {
        ValueType::Object
    }
}
