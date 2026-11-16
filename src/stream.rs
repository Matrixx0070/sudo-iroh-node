//! Stream halves. Each direction copies the payload exactly once: a write copies the caller's
//! Buffer/Uint8Array into refcounted bytes on the JS thread (so JS may reuse it immediately) and
//! hands those to QUIC; a read gathers QUIC's received chunks without copying and copies them into
//! one JS-owned Buffer on the JS thread.
//!
//! Every operation first takes its turn on the half's FIFO lock, on the JS thread, so operations
//! run in call order even when JS does not await them. If the turn is free the operation is tried
//! right there: a write QUIC can buffer, or a read of data QUIC already holds, settles its promise
//! without leaving the JS thread. Only an operation that has to wait (flow control, no data yet,
//! an earlier operation still running) moves to the tokio runtime.
//!
//! `stop` and `reset` first raise the half's cancel signal, which makes a waiting read or write
//! give up its turn, so they never wait on a peer that stays silent or withholds flow-control
//! credit.

use std::{
    cell::Cell,
    collections::VecDeque,
    future::Future,
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
use iroh::endpoint::{self, ReadError, VarInt};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

use crate::js_err;

// Resolves once `stop` or `reset` has been requested for this half. The sender lives as long as
// the half itself, so a dropped sender means nobody can cancel and the wait never resolves.
async fn cancelled(mut rx: watch::Receiver<bool>) {
    if rx.wait_for(|requested| *requested).await.is_err() {
        std::future::pending::<()>().await;
    }
}

// Polls a future once without registering for a wakeup. Only for futures that are cancel safe and
// are either dropped or polled again from a real task afterwards.
fn poll_once<F: Future>(fut: Pin<&mut F>) -> Poll<F::Output> {
    fut.poll(&mut Context::from_waker(Waker::noop()))
}

// An operation's place in a half's FIFO: the lock itself, or the queued wait for it.
enum Turn<T> {
    Now(OwnedMutexGuard<T>),
    Later(Pin<Box<dyn Future<Output = OwnedMutexGuard<T>> + Send>>),
}

impl<T: Send + 'static> Turn<T> {
    // Called on the JS thread. tokio's mutex is fair, and the wait is queued here (by the first
    // poll) rather than whenever a runtime thread gets to it, so turns follow JS call order.
    fn take(lock: &Arc<Mutex<T>>) -> Self {
        if let Ok(guard) = lock.clone().try_lock_owned() {
            return Self::Now(guard);
        }
        let mut wait = Box::pin(lock.clone().lock_owned());
        match poll_once(wait.as_mut()) {
            Poll::Ready(guard) => Self::Now(guard),
            Poll::Pending => Self::Later(wait),
        }
    }

    async fn guard(self) -> OwnedMutexGuard<T> {
        match self {
            Self::Now(guard) => guard,
            Self::Later(wait) => wait.await,
        }
    }
}

// Work the JS thread may settle in a row without going back to the event loop. An `await` on an
// already settled promise only runs microtasks, so without a bound a loop over a busy stream
// would starve timers and other I/O. Past the budget the next operation takes the runtime path,
// whose completion is delivered through the event loop.
const INLINE_OPS: u32 = 64;
const INLINE_BYTES: usize = 1 << 20;

thread_local! {
    static INLINE_BUDGET: Cell<(u32, usize)> = const { Cell::new((INLINE_OPS, INLINE_BYTES)) };
}

// Whether the JS thread may try an operation in place. A refusal refills the budget, because the
// caller then goes through the event loop.
fn inline_allowed() -> bool {
    INLINE_BUDGET.with(|b| {
        let (ops, bytes) = b.get();
        let allowed = ops > 0 && bytes > 0;
        if !allowed {
            b.set((INLINE_OPS, INLINE_BYTES));
        }
        allowed
    })
}

fn inline_spent(bytes: usize) {
    INLINE_BUDGET.with(|b| {
        let (ops, left) = b.get();
        b.set((ops.saturating_sub(1), left.saturating_sub(bytes)));
    });
}

// A promise settled on the JS thread with `value`.
fn resolved<'env, T>(env: &'env Env, value: sys::napi_value) -> Result<PromiseRaw<'env, T>> {
    let mut deferred = std::ptr::null_mut();
    let mut promise = std::ptr::null_mut();
    check_status!(
        unsafe { sys::napi_create_promise(env.raw(), &mut deferred, &mut promise) },
        "failed to create a promise"
    )?;
    check_status!(
        unsafe { sys::napi_resolve_deferred(env.raw(), deferred, value) },
        "failed to resolve a promise"
    )?;
    Ok(PromiseRaw::new(env.raw(), promise))
}

fn resolved_void<'env>(env: &'env Env) -> Result<PromiseRaw<'env, ()>> {
    let mut undefined = std::ptr::null_mut();
    check_status!(unsafe { sys::napi_get_undefined(env.raw(), &mut undefined) })?;
    resolved(env, undefined)
}

// A promise that rejects with `reason` (through the runtime: errors are not a hot path).
fn rejected<'env, T: ToNapiValue + Send + 'static>(
    env: &'env Env,
    reason: impl std::fmt::Display,
) -> Result<PromiseRaw<'env, T>> {
    let err = js_err(reason);
    env.spawn_future(async move { Err::<T, _>(err) })
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

    // Writes every chunk in order. `chunks` is `[Bytes; 1]` for `write` and a Vec for `writeMany`.
    fn submit<'env, C>(&self, env: &'env Env, mut chunks: C) -> Result<PromiseRaw<'env, ()>>
    where
        C: AsMut<[Bytes]> + Send + 'static,
    {
        if *self.cancel.borrow() {
            return rejected(env, "stream reset");
        }
        let mut turn = Turn::take(&self.inner);
        // Chunks QUIC took in full on the JS thread; the first of the rest may be partly written.
        let mut done = 0;
        if let Turn::Now(stream) = &mut turn
            && inline_allowed()
        {
            let all = chunks.as_mut();
            let total = all.len();
            if total == 0 {
                return resolved_void(env);
            }
            let mut rest = &mut *all;
            let wrote = poll_once(pin!(stream.write_many_chunks(&mut rest)));
            done = total - rest.len();
            match wrote {
                Poll::Ready(Ok(bytes)) => {
                    inline_spent(bytes);
                    if done == total {
                        return resolved_void(env);
                    }
                }
                Poll::Ready(Err(e)) => return rejected(env, e),
                // Flow control has no room yet: nothing was written.
                Poll::Pending => {}
            }
        }
        let cancel = self.cancel.subscribe();
        env.spawn_future(async move {
            // A reset abandons the write; otherwise wait for QUIC to accept every byte.
            tokio::select! {
                biased;
                () = cancelled(cancel) => Err(js_err("stream reset")),
                written = async {
                    let mut stream = turn.guard().await;
                    stream.write_all_chunks(&mut chunks.as_mut()[done..]).await
                } => written.map_err(js_err),
            }
        })
    }
}

#[napi]
impl SendStream {
    /// Writes all bytes; resolves when QUIC has accepted them (flow-control backpressure). The
    /// bytes are copied before this returns, so the caller may reuse `data` right away.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn write<'env>(&self, env: &'env Env, data: &[u8]) -> Result<PromiseRaw<'env, ()>> {
        // The one copy: QUIC keeps these bytes (refcounted, never copied again) until they are
        // acknowledged.
        if data.is_empty() {
            // QUIC has no zero-length write; an empty one still takes its turn and reports errors.
            return self.submit(env, [Bytes::new(); 0]);
        }
        self.submit(env, [Bytes::copy_from_slice(data)])
    }

    /// Writes several buffers in order as one operation: one promise and one pass through QUIC
    /// for the whole batch. Use it when many small frames are ready at once.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn write_many<'env>(
        &self,
        env: &'env Env,
        data: Vec<&[u8]>,
    ) -> Result<PromiseRaw<'env, ()>> {
        let chunks: Vec<Bytes> = data
            .into_iter()
            .filter(|d| !d.is_empty())
            .map(Bytes::copy_from_slice)
            .collect();
        self.submit(env, chunks)
    }

    /// Ends the send side cleanly (FIN), after every write issued before it.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn finish<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, ()>> {
        match Turn::take(&self.inner) {
            Turn::Now(mut stream) => match stream.finish() {
                Ok(()) => resolved_void(env),
                Err(e) => rejected(env, e),
            },
            turn => env.spawn_future(async move { turn.guard().await.finish().map_err(js_err) }),
        }
    }

    /// Abandons the send side with an application error code. A pending write rejects first, so
    /// the reset is not held up by a peer that withholds flow-control credit.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn reset<'env>(&self, env: &'env Env, code: u32) -> Result<PromiseRaw<'env, ()>> {
        self.cancel.send_replace(true);
        let code = VarInt::from_u32(code);
        match Turn::take(&self.inner) {
            Turn::Now(mut stream) => match stream.reset(code) {
                Ok(()) => resolved_void(env),
                Err(e) => rejected(env, e),
            },
            turn => env.spawn_future(async move { turn.guard().await.reset(code).map_err(js_err) }),
        }
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

    fn stop(&mut self, code: VarInt) -> std::result::Result<(), endpoint::ClosedStream> {
        self.carry.clear();
        self.stream.stop(code)
    }
}

/// Receiving half of a bidirectional stream.
#[napi]
pub struct RecvStream {
    inner: Arc<Mutex<RecvInner>>,
    cancel: watch::Sender<bool>,
}

impl RecvStream {
    pub(crate) fn new(r: endpoint::RecvStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RecvInner {
                stream: r,
                carry: VecDeque::new(),
                slots: Vec::new(),
            })),
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
    pub fn read<'env>(
        &self,
        env: &'env Env,
        max: u32,
    ) -> Result<PromiseRaw<'env, Option<ReadBytes>>> {
        if max == 0 {
            return rejected(env, "max must be > 0");
        }
        if *self.cancel.borrow() {
            return rejected(env, "stream stopped");
        }
        let max = (max as usize).min(MAX_READ);
        let mut turn = Turn::take(&self.inner);
        if let Turn::Now(inner) = &mut turn
            && inline_allowed()
        {
            match poll_once(pin!(inner.read(max))) {
                Poll::Ready(Ok(read)) => {
                    inline_spent(read.as_ref().map_or(0, |r| r.len));
                    let value = unsafe { ToNapiValue::to_napi_value(env.raw(), read) }?;
                    return resolved(env, value);
                }
                Poll::Ready(Err(e)) => return rejected(env, e),
                // Nothing received yet: nothing was consumed.
                Poll::Pending => {}
            }
        }
        let cancel = self.cancel.subscribe();
        env.spawn_future(async move {
            // A stop abandons the read; otherwise wait for data, end of stream or a peer reset.
            tokio::select! {
                biased;
                () = cancelled(cancel) => Err(js_err("stream stopped")),
                read = async { turn.guard().await.read(max).await } => read.map_err(js_err),
            }
        })
    }

    /// Asks the peer to stop sending, with an application error code. A pending read rejects
    /// first, so STOP_SENDING is not held up by a peer that stays silent.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn stop<'env>(&self, env: &'env Env, code: u32) -> Result<PromiseRaw<'env, ()>> {
        self.cancel.send_replace(true);
        let code = VarInt::from_u32(code);
        match Turn::take(&self.inner) {
            Turn::Now(mut inner) => match inner.stop(code) {
                Ok(()) => resolved_void(env),
                Err(e) => rejected(env, e),
            },
            turn => env.spawn_future(async move { turn.guard().await.stop(code).map_err(js_err) }),
        }
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
