# Firefox bug: incoming WebTransport streams stall at 2

Firefox stops delivering **server-initiated** WebTransport streams to the page
after **2**, when the application reads a stream's body before pulling the next
stream from `incomingBidirectionalStreams` / `incomingUnidirectionalStreams`.
Chrome is unaffected. Reproduce with the harness in this directory
(`../test.html`, scenario `server-bi-5`); see [`../TESTING.md`](../TESTING.md).

## Where it breaks (traced through every layer)

| Layer | Streams handled | Evidence |
|---|---|---|
| neqo / HTTP-3 | 5 ✅ | `OnIncomingWebTransportStream` ×5 (parent log) |
| Parent DOM → content IPC | 5 ✅ | `Sending BidirectionalStream pipe to content` ×5 (parent log) |
| Content process receive | 5 ✅ | `NewBidirectionalStream()` ×5, `NotifyIncomingStream` → 4 (content log) |
| **`IncomingBidirectionalStreams` ReadableStream** | **2 ❌** | `Pull` / `Enqueuing bidirectional stream` only ×2 (content log) |

Every layer delivers all 5 streams. The JS-facing `IncomingBidirectionalStreams`
ReadableStream only **enqueues 2**: its `Pull` callback fires twice and is never
invoked again, even though 3+ streams sit in the internal backlog and the page
keeps calling `reader.read()`. The incoming-streams source does not re-pull from
its backlog once the ReadableStream's `desiredSize` drops after the initial fill.

This explains the scenarios:

- `server-bi-5` / `server-uni-3` — read each body, then pull next → **stall at 2**.
- `server-bi-probe-5` — pull all 5 stream objects up front (while `Pull` is still
  being driven), then read them → **all 5** delivered.
- `server-bi-serial-10` — server opens one at a time, never backlogs → **passes**.

## Root cause

Traced to the content-process pull algorithm in
`dom/webtransport/api/WebTransportStreams.cpp`: the `length > 0` fast path of
`PullCallbackImpl` returns a pull promise that is never resolved, wedging the
ReadableStream's pull loop once a backlog forms. Full analysis + suggested fix:
[`root-cause.md`](root-cause.md).

## Files

- [`root-cause.md`](root-cause.md) — code-level analysis and proposed patch.
- [`parent-process.log`](parent-process.log) — `MOZ_LOG=timestamp,nsHttp:5,WebTransport:5`,
  parent process. Shows neqo/HTTP-3 receiving all 5 and forwarding all 5 to content.
- [`content-process.log`](content-process.log) — `MOZ_LOG=timestamp,WebTransport:5`,
  content process. Shows all 5 arriving but only 2 enqueued to JS (`Pull` stops).

Captured on Firefox, 2026-06-10, against the `test-server` example in this repo.
