//! Stream halves. Each direction copies the payload exactly once: a write copies the caller's
//! Buffer/Uint8Array into refcounted bytes on the JS thread (so JS may reuse it immediately) and
//! hands those to QUIC; a read gathers QUIC's received chunks without copying and copies them into
//! one JS-owned Buffer on the JS thread.
//!
//! Send side: a writer task owns the QUIC stream. `write` appends to the stream's queue on the JS
//! thread and, while the queue is short, settles its promise right there; the task passes
//! everything queued to QUIC in one call. JS never touches the connection lock, a run of small
//! writes is batched, and writes keep their call order even when JS does not await them.
//!
//! Receive side: a reader task owns the QUIC stream and moves what has arrived (up to about
//! 1 MiB ahead of JS) into the stream's queue. `read` takes from the queue on the JS thread and
//! settles its promise right there; only a read that finds nothing waits for the task.
//!
//! Neither half sits behind a lock that is held across an await, and JS never takes QUIC's
//! connection lock. `stop` and `reset` settle the waiting reads or writes themselves and wake the
//! task, so they never wait on a peer that stays silent or withholds flow-control credit.

use std::{cell::Cell, collections::VecDeque, sync::Arc};

use bytes::Bytes;
use iroh::endpoint::{self, ReadError, VarInt};
use napi::{JsDeferred, bindgen_prelude::*};
use napi_derive::napi;
use tokio::sync::{Notify, watch};

use crate::js_err;

// Resolves once `stop` or `reset` has been requested for this half. The sender lives as long as
// the half itself, so a dropped sender means nobody can cancel and the wait never resolves.
async fn cancelled(mut rx: watch::Receiver<bool>) {
    if rx.wait_for(|requested| *requested).await.is_err() {
        std::future::pending::<()>().await;
    }
}

// Work the JS thread may settle in a row without going back to the event loop. An `await` on an
// already settled promise only runs microtasks, so without a bound a loop over a busy stream
// would starve timers and other I/O. Past the budget the next operation settles through the
// event loop instead.
const INLINE_OPS: u32 = 256;
const INLINE_BYTES: usize = 1 << 20;

thread_local! {
    static INLINE_BUDGET: Cell<(u32, usize)> = const { Cell::new((INLINE_OPS, INLINE_BYTES)) };
}

// Whether the JS thread may settle one more operation in place. A refusal refills the budget,
// because the caller then goes through the event loop.
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

// A void promise the writer task settles from the runtime.
type Settle = JsDeferred<(), fn(Env) -> Result<()>>;

fn pending<'env>(env: &'env Env) -> Result<(Settle, PromiseRaw<'env, ()>)> {
    let (settle, promise) = env.create_deferred()?;
    Ok((settle, PromiseRaw::new(env.raw(), promise.raw())))
}

fn settle(settle: Settle, outcome: std::result::Result<(), impl std::fmt::Display>) {
    match outcome {
        Ok(()) => settle.resolve(|_| Ok(())),
        Err(e) => settle.reject(js_err(e)),
    }
}

// Bytes a stream may hold for QUIC before `write` stops settling at once. Beyond it a write
// waits until QUIC has accepted its bytes, which is the backpressure towards JS.
const HIGH_WATER: u64 = 1 << 20;

// What noq reports for an operation on a send side that is already finished or reset.
const CLOSED: &str = "closed stream";

// What JS has asked of a send side and the writer task has not done yet.
#[derive(Default)]
struct SendState {
    // Written by JS, not yet taken by the task.
    queue: Vec<Bytes>,
    // Bytes ever queued and bytes QUIC has accepted so far.
    queued: u64,
    accepted: u64,
    // Writes (and flushes) to settle once `accepted` reaches their position, in order.
    waiters: VecDeque<(u64, Settle)>,
    // `finish` was called: later writes are refused. The task sends the FIN after the queue.
    finishing: bool,
    finish: Option<Settle>,
    reset: Option<(VarInt, Settle)>,
    // First failure, or the reset. Every later write and the finish reject with it.
    failed: Option<String>,
    // The JS object is gone: the task flushes what is queued and ends.
    dropped: bool,
}

struct SendShared {
    state: std::sync::Mutex<SendState>,
    // Wakes the task: something was queued or requested.
    work: Notify,
    // Raised by `reset`, so a write waiting on flow control gives up.
    cancel: watch::Sender<bool>,
}

impl SendShared {
    fn state(&self) -> std::sync::MutexGuard<'_, SendState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    // Records a failure and rejects everything that waits on bytes that will never be sent.
    fn fail(&self, why: String) {
        let waiters = {
            let mut st = self.state();
            st.queue.clear();
            st.accepted = st.queued;
            st.failed.get_or_insert_with(|| why.clone());
            std::mem::take(&mut st.waiters)
        };
        for (_, waiter) in waiters {
            waiter.reject(js_err(&why));
        }
    }

    // The writer task: sole owner of the QUIC stream. It hands everything JS queued since its
    // last pass to QUIC in one call, so a run of small writes costs one connection lock and one
    // transmit wakeup instead of one each.
    async fn drive(self: Arc<Self>, mut stream: endpoint::SendStream) {
        enum Next {
            Write,
            Finish(Settle, Option<String>),
            Reset(VarInt, Settle),
            Park,
            End,
        }
        let mut batch = Vec::new();
        loop {
            let next = {
                let mut st = self.state();
                if let Some((code, done)) = st.reset.take() {
                    Next::Reset(code, done)
                } else if !st.queue.is_empty() {
                    batch.clear();
                    std::mem::swap(&mut st.queue, &mut batch);
                    Next::Write
                } else if let Some(done) = st.finish.take() {
                    Next::Finish(done, st.failed.clone())
                } else if st.dropped {
                    Next::End
                } else {
                    Next::Park
                }
            };
            match next {
                Next::Park => self.work.notified().await,
                // Dropping the stream finishes it if nothing else did.
                Next::End => return,
                Next::Finish(done, failed) => {
                    let finished = stream.finish().map_err(|e| e.to_string());
                    // A FIN after lost writes is not a clean end: report what was lost.
                    settle(done, failed.map_or(finished, Err));
                }
                Next::Reset(code, done) => settle(done, stream.reset(code)),
                Next::Write => {
                    let mut rest = &mut batch[..];
                    while !rest.is_empty() {
                        let wrote = tokio::select! {
                            biased;
                            // The reset request is in the state; the next pass serves it.
                            () = cancelled(self.cancel.subscribe()) => break,
                            wrote = stream.write_many_chunks(&mut rest) => wrote,
                        };
                        match wrote {
                            Ok(bytes) => self.accepted(bytes as u64),
                            Err(e) => {
                                self.fail(e.to_string());
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    // QUIC accepted `bytes` more: settles the writes that were waiting for them.
    fn accepted(&self, bytes: u64) {
        let mut ready = Vec::new();
        {
            let mut st = self.state();
            // A reset may have written the rest off in the meantime.
            st.accepted = st.queued.min(st.accepted + bytes);
            while st.waiters.front().is_some_and(|w| w.0 <= st.accepted) {
                ready.extend(st.waiters.pop_front());
            }
        }
        for (_, waiter) in ready {
            waiter.resolve(|_| Ok(()));
        }
    }
}

/// Sending half of a bidirectional stream.
#[napi]
pub struct SendStream {
    shared: Arc<SendShared>,
}

impl Drop for SendStream {
    fn drop(&mut self) {
        self.shared.state().dropped = true;
        self.shared.work.notify_one();
    }
}

impl SendStream {
    pub(crate) fn new(s: endpoint::SendStream) -> Self {
        let shared = Arc::new(SendShared {
            state: Default::default(),
            work: Notify::new(),
            cancel: watch::Sender::new(false),
        });
        spawn(shared.clone().drive(s));
        Self { shared }
    }

    // Queues `chunks` (`bytes` in total, none empty) behind everything written before.
    fn submit<'env>(
        &self,
        env: &'env Env,
        chunks: impl IntoIterator<Item = Bytes>,
        bytes: usize,
    ) -> Result<PromiseRaw<'env, ()>> {
        let mut st = self.shared.state();
        if let Some(why) = &st.failed {
            return rejected(env, why.clone());
        }
        if st.finishing {
            return rejected(env, CLOSED);
        }
        if bytes == 0 {
            return resolved_void(env);
        }
        let backlog = st.queued + bytes as u64 - st.accepted;
        // Settle now while the backlog is small; otherwise when QUIC has taken these bytes.
        let promise = if backlog <= HIGH_WATER && inline_allowed() {
            inline_spent(bytes);
            resolved_void(env)?
        } else {
            let (waiter, promise) = pending(env)?;
            let position = st.queued + bytes as u64;
            st.waiters.push_back((position, waiter));
            promise
        };
        let idle = st.queue.is_empty();
        st.queue.extend(chunks);
        st.queued += bytes as u64;
        drop(st);
        if idle {
            self.shared.work.notify_one();
        }
        Ok(promise)
    }
}

#[napi]
impl SendStream {
    /// Queues the bytes for QUIC, in call order. They are copied before this returns, so the
    /// caller may reuse `data` right away.
    ///
    /// Resolves at once while less than 1 MiB is waiting for QUIC, and otherwise when QUIC has
    /// accepted these bytes (flow-control backpressure). A failure of bytes that were queued when
    /// their write resolved (peer stopped the stream, connection lost) rejects the next `write`,
    /// `flush` or `finish`.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn write<'env>(&self, env: &'env Env, data: &[u8]) -> Result<PromiseRaw<'env, ()>> {
        // The one copy: QUIC keeps these bytes (refcounted, never copied again) until they are
        // acknowledged.
        let chunk = (!data.is_empty()).then(|| Bytes::copy_from_slice(data));
        self.submit(env, chunk, data.len())
    }

    /// Like `write` for several buffers in order, with one promise for the whole batch.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn write_many<'env>(
        &self,
        env: &'env Env,
        data: Vec<&[u8]>,
    ) -> Result<PromiseRaw<'env, ()>> {
        let bytes = data.iter().map(|d| d.len()).sum();
        let chunks = data
            .into_iter()
            .filter(|d| !d.is_empty())
            .map(Bytes::copy_from_slice);
        self.submit(env, chunks, bytes)
    }

    /// Resolves when QUIC has accepted everything written so far; rejects if any of it failed.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn flush<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, ()>> {
        let mut st = self.shared.state();
        if let Some(why) = &st.failed {
            return rejected(env, why.clone());
        }
        if st.accepted == st.queued {
            return resolved_void(env);
        }
        let (waiter, promise) = pending(env)?;
        let position = st.queued;
        st.waiters.push_back((position, waiter));
        Ok(promise)
    }

    /// Ends the send side cleanly (FIN) after everything written before it. Rejects if any of
    /// those bytes failed.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn finish<'env>(&self, env: &'env Env) -> Result<PromiseRaw<'env, ()>> {
        let mut st = self.shared.state();
        if st.finishing {
            return rejected(env, CLOSED);
        }
        let (done, promise) = pending(env)?;
        st.finishing = true;
        st.finish = Some(done);
        drop(st);
        self.shared.work.notify_one();
        Ok(promise)
    }

    /// Abandons the send side with an application error code. Pending writes reject first and
    /// unsent bytes are dropped, so the reset is not held up by a peer that withholds
    /// flow-control credit.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn reset<'env>(&self, env: &'env Env, code: u32) -> Result<PromiseRaw<'env, ()>> {
        let (done, promise) = pending(env)?;
        self.shared.fail("stream reset".into());
        let earlier = {
            let mut st = self.shared.state();
            st.failed = Some("stream reset".into());
            st.reset.replace((VarInt::from_u32(code), done))
        };
        if let Some((_, earlier)) = earlier {
            // Two resets in a row: the task serves the later one, QUIC keeps the first code.
            earlier.reject(js_err(CLOSED));
        }
        self.shared.cancel.send_replace(true);
        self.shared.work.notify_one();
        Ok(promise)
    }
}

/// Largest `max` a single read honors; larger requests are clamped.
pub const MAX_READ: usize = 4 << 20;

// Bytes a receive side takes from QUIC ahead of JS. Beyond it the data stays in QUIC, whose flow
// control then holds the peer back.
const READ_AHEAD: usize = 1 << 20;

// QUIC hands stream data out as one refcounted chunk per received STREAM frame, so a chunk is
// about one packet. Asking for `bytes / MIN_CHUNK` chunks lets one call reach `bytes`.
const MIN_CHUNK: usize = 1024;

// A read promise the reader task settles from the runtime.
type ReadResolver = Box<dyn FnOnce(Env) -> Result<Option<ReadBytes>> + Send>;
type ReadSettle = JsDeferred<Option<ReadBytes>, ReadResolver>;

fn pending_read<'env>(env: &'env Env) -> Result<(ReadSettle, PromiseRaw<'env, Option<ReadBytes>>)> {
    let (settle, promise) = env.create_deferred()?;
    Ok((settle, PromiseRaw::new(env.raw(), promise.raw())))
}

fn settle_read(settle: ReadSettle, outcome: std::result::Result<Option<ReadBytes>, String>) {
    match outcome {
        Ok(read) => settle.resolve(Box::new(move |_| Ok(read))),
        Err(e) => settle.reject(js_err(e)),
    }
}

// What the reader task has received for JS, and what JS has asked of it.
#[derive(Default)]
struct RecvState {
    // Received, not yet read by JS.
    queue: VecDeque<Bytes>,
    queued: usize,
    // Reads waiting for data, in call order, with their `max`. Only while `queue` is empty.
    waiters: VecDeque<(usize, ReadSettle)>,
    // How the stream ended: cleanly, or with the error every later read rejects with.
    end: Option<std::result::Result<(), String>>,
    stop: Option<(VarInt, Settle)>,
    // The task is parked on a full queue and wants a wakeup once JS has made room.
    full: bool,
    // The JS object is gone: the task ends once no read is waiting.
    dropped: bool,
}

impl RecvState {
    // Takes up to `max` bytes off the queue.
    fn take(&mut self, max: usize) -> ReadBytes {
        let mut out = ReadBytes::default();
        while out.len < max {
            let Some(mut chunk) = self.queue.pop_front() else {
                break;
            };
            let room = max - out.len;
            if chunk.len() > room {
                let rest = chunk.split_off(room);
                self.queue.push_front(rest);
            }
            out.push(chunk);
        }
        self.queued -= out.len;
        out
    }
}

struct RecvShared {
    state: std::sync::Mutex<RecvState>,
    // Wakes the task: room in the queue, a stop, or the JS object is gone.
    work: Notify,
}

impl RecvShared {
    fn state(&self) -> std::sync::MutexGuard<'_, RecvState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    // The reader task: sole owner of the QUIC stream. It moves what QUIC has received into the
    // queue (refcounted, no copy), every ready chunk under one connection lock, so a read from
    // JS only has to take from the queue.
    async fn drive(self: Arc<Self>, mut stream: endpoint::RecvStream) {
        enum Next {
            Read(usize),
            Stop(VarInt, Settle),
            Park,
            End,
        }
        let mut slots = vec![Bytes::new(); READ_AHEAD / MIN_CHUNK];
        loop {
            let next = {
                let mut st = self.state();
                if let Some((code, done)) = st.stop.take() {
                    Next::Stop(code, done)
                } else if st.dropped && st.waiters.is_empty() {
                    Next::End
                } else if st.end.is_some() {
                    Next::Park
                } else if st.queued >= READ_AHEAD {
                    st.full = true;
                    Next::Park
                } else {
                    Next::Read((READ_AHEAD - st.queued).div_ceil(MIN_CHUNK))
                }
            };
            match next {
                Next::Park => self.work.notified().await,
                // Dropping the stream tells the peer to stop sending, unless it has ended.
                Next::End => return,
                Next::Stop(code, done) => settle(done, stream.stop(code)),
                Next::Read(want) => {
                    let read = tokio::select! {
                        biased;
                        // A stop or a drop: the next pass serves it.
                        () = self.work.notified() => continue,
                        read = stream.read_many_chunks(&mut slots[..want]) => read,
                    };
                    self.received(read.map(|n| n.map(|n| &mut slots[..n])));
                }
            }
        }
    }

    // Queues what the task read and serves the reads that were waiting for it.
    fn received(&self, read: std::result::Result<Option<&mut [Bytes]>, ReadError>) {
        let mut ready = Vec::new();
        {
            let mut st = self.state();
            if st.end.is_some() {
                // Stopped in the meantime: the data is no longer wanted.
                return;
            }
            match read {
                Ok(Some(chunks)) => {
                    for chunk in chunks.iter_mut().map(std::mem::take) {
                        if !chunk.is_empty() {
                            st.queued += chunk.len();
                            st.queue.push_back(chunk);
                        }
                    }
                    while !st.queue.is_empty() {
                        let Some((max, waiter)) = st.waiters.pop_front() else {
                            break;
                        };
                        ready.push((waiter, Ok(Some(st.take(max)))));
                    }
                }
                Ok(None) => {
                    st.end = Some(Ok(()));
                    ready.extend(st.waiters.drain(..).map(|w| (w.1, Ok(None))));
                }
                Err(e) => {
                    let why = e.to_string();
                    ready.extend(st.waiters.drain(..).map(|w| (w.1, Err(why.clone()))));
                    st.end = Some(Err(why));
                }
            }
        }
        for (waiter, outcome) in ready {
            settle_read(waiter, outcome);
        }
    }
}

/// Receiving half of a bidirectional stream.
#[napi]
pub struct RecvStream {
    shared: Arc<RecvShared>,
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        self.shared.state().dropped = true;
        self.shared.work.notify_one();
    }
}

impl RecvStream {
    pub(crate) fn new(r: endpoint::RecvStream) -> Self {
        let shared = Arc::new(RecvShared {
            state: Default::default(),
            work: Notify::new(),
        });
        spawn(shared.clone().drive(r));
        Self { shared }
    }
}

#[napi]
impl RecvStream {
    /// Reads whatever has arrived (at least 1 byte, at most `max`, which is clamped to 4 MiB) as
    /// one Buffer covering many packets; waits only if nothing has arrived yet. Resolves null at
    /// end of stream. Rejects if the peer reset the stream (message contains the reset code),
    /// after the bytes received before the reset have been read.
    ///
    /// About 1 MiB per stream is taken from QUIC ahead of the reads; a slower reader holds the
    /// peer back through QUIC flow control.
    #[napi(ts_return_type = "Promise<Buffer | null>")]
    pub fn read<'env>(
        &self,
        env: &'env Env,
        max: u32,
    ) -> Result<PromiseRaw<'env, Option<ReadBytes>>> {
        if max == 0 {
            return rejected(env, "max must be > 0");
        }
        let max = (max as usize).min(MAX_READ);
        let mut st = self.shared.state();
        let read = if !st.queue.is_empty() {
            let bytes = st.take(max);
            // Half the queue is free again: worth waking the task for.
            if st.full && st.queued <= READ_AHEAD / 2 {
                st.full = false;
                self.shared.work.notify_one();
            }
            Some(bytes)
        } else {
            match &st.end {
                Some(Ok(())) => None,
                Some(Err(why)) => return rejected(env, why.clone()),
                None => {
                    let (waiter, promise) = pending_read(env)?;
                    st.waiters.push_back((max, waiter));
                    return Ok(promise);
                }
            }
        };
        drop(st);
        if inline_allowed() {
            inline_spent(read.as_ref().map_or(0, |r| r.len));
            let value = unsafe { ToNapiValue::to_napi_value(env.raw(), read) }?;
            return resolved(env, value);
        }
        // Over the inline budget: the same result, settled through the event loop.
        let (later, promise) = pending_read(env)?;
        settle_read(later, Ok(read));
        Ok(promise)
    }

    /// Asks the peer to stop sending, with an application error code. Pending and later reads
    /// reject and unread bytes are dropped, so STOP_SENDING is not held up by a peer that stays
    /// silent.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn stop<'env>(&self, env: &'env Env, code: u32) -> Result<PromiseRaw<'env, ()>> {
        const STOPPED: &str = "stream stopped";
        let (done, promise) = pending(env)?;
        let (waiters, earlier) = {
            let mut st = self.shared.state();
            st.queue.clear();
            st.queued = 0;
            st.end = Some(Err(STOPPED.into()));
            let waiters = std::mem::take(&mut st.waiters);
            (waiters, st.stop.replace((VarInt::from_u32(code), done)))
        };
        for (_, waiter) in waiters {
            waiter.reject(js_err(STOPPED));
        }
        if let Some((_, earlier)) = earlier {
            // Two stops in a row: the task serves the later one, QUIC keeps the first code.
            earlier.reject(js_err(CLOSED));
        }
        self.shared.work.notify_one();
        Ok(promise)
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
