# Root cause: `IncomingBidirectionalStreams` pull promise is never resolved on the fast path

**TL;DR** — Firefox stops delivering server-initiated WebTransport streams to the
page after the first one whenever a backlog has formed, because
`WebTransportIncomingStreamsAlgorithms::PullCallbackImpl` returns an **unresolved
promise** on its synchronous (`length > 0`) path. A ReadableStream won't call
`Pull` again until the previous pull promise settles, so the incoming-streams
reader wedges permanently.

## Location

[`dom/webtransport/api/WebTransportStreams.cpp`](https://searchfox.org/mozilla-central/source/dom/webtransport/api/WebTransportStreams.cpp)
— `WebTransportIncomingStreamsAlgorithms::PullCallbackImpl` / `BuildStream`.

```cpp
RefPtr<Promise> promise = Promise::CreateInfallible(...);   // the pull promise
auto length = mTransport->mBidirectionalStreams.Length();
if (length == 0) {
  // SLOW PATH — wait for a stream, then BuildStream via a chained promise.
  mCallback = promise;
  Result<...> returnResult = promise->ThenWithCycleCollectedArgs(
      [](...) { self->BuildStream(aCx, aRv); return nullptr; }, ...);
  return returnResult.unwrap().forget();   // ← a promise that DOES resolve
}
self->BuildStream(aCx, aRv);
return promise.forget();                    // ← FAST PATH: `promise` is NEVER resolved
```

`BuildStream()` enqueues the stream but never resolves `promise`, even though the
spec step it implements is annotated in the code as *"Step 7.3: Resolve p with
undefined."* (the comment is present; the resolve is not).

## Why it wedges

Per the Streams spec, `ReadableStreamDefaultControllerCallPullIfNeeded` sets
`pulling = true`, calls the pull algorithm, and only clears `pulling` / honours a
queued `pullAgain` **when the returned promise fulfills**. The `length > 0` path
returns a promise that never fulfills, so after it runs once `pulling` stays
`true` forever and `Pull` is never invoked again — no matter how many streams are
backlogged or how many times the page calls `reader.read()`.

The `length == 0` path returns a *chained* promise (`ThenWithCycleCollectedArgs`)
that resolves once `BuildStream` runs, so it behaves correctly. The bug is the
asymmetry between the two paths.

## Why it reproduces exactly as observed

`length > 0` means a stream is already in the backlog when `Pull` runs — i.e. the
app let streams pile up. MOZ_LOG (content process, `WebTransport:5`) shows all 5
streams arriving (`NewBidirectionalStream()` ×5, `NotifyIncomingStream` backlog
→ 4) but only **2** `Enqueuing bidirectional stream` before `Pull` goes silent.

| Scenario | What `Pull` sees | Path taken | Result |
|---|---|---|---|
| `server-bi-5` / `server-uni-3` (read body, *then* pull next) | backlog builds while reading bodies; 2nd pull finds `length > 0` | fast path → unresolved → **wedged** | stalls at 2 |
| `server-bi-probe-5` (pull all stream objects first) | queue stays drained; every pull finds `length == 0` | slow path (resolves) | all 5 |
| `server-bi-serial-10` (server opens one at a time) | backlog never forms; always `length == 0` | slow path | passes |

It also explains the precise "stalls at **2**": stream 1 is delivered via the
working `length == 0` path; by the second pull a backlog exists, the `length > 0`
path runs, returns the dead promise, and pull never fires again.

This is direction-agnostic (uni hits the same code), QUIC/HTTP-3 and the parent
process deliver all N streams (see `parent-process.log`), and Chrome is
unaffected — consistent with the defect being solely in this content-process
pull algorithm.

## Suggested fix

Resolve the pull promise on the synchronous path (ideally make `BuildStream` own
the resolve so both paths are consistent):

```cpp
  self->BuildStream(aCx, aRv);
  if (aRv.Failed()) {
    promise->MaybeReject(aRv.StealNSResult());
    return promise.forget();
  }
  promise->MaybeResolveWithUndefined();   // the missing Step 7.3
  return promise.forget();
```

## Repro

Self-contained, no public server needed: <https://github.com/moq-dev/web-transport/pull/251>.
Run the harness in Firefox and compare `server-bi-5` (stalls at 2) with
`server-bi-probe-5` (all 5). Traces in this directory: `parent-process.log`,
`content-process.log`.

> Caveat: code/line references are from `mozilla-central` (GitHub `mozilla/gecko-dev`
> mirror, read June 2026). The logic has clearly been in place a while, but
> confirm against the exact Firefox build before filing.
