# Browser WebTransport test harness

A reproducible battery of WebTransport scenarios for chasing browser-specific
bugs — primarily:

- **Firefox** breaking on **server-initiated bidirectional streams** (especially
  the 2nd one).
- **Chrome** "Aww, Snap! Error code 11" (a renderer **crash** = SIGSEGV) around
  **explicit session close**, on connect or disconnect.

## Findings so far

- **Chrome (this quinn server):** 25/25 PASS, including every explicit-close path
  and CONNECT rejection (`/reject/<code>`). The "Aww, Snap! error 11" crash did
  **not** reproduce here — it likely needs a different reject/close shape or a
  different backend than `web-transport-quinn`.
- **Firefox:** server-initiated streams stall at **2** — but it is **not** a
  stream-credit problem. Root cause: **`incomingBidirectionalStreams` /
  `incomingUnidirectionalStreams` backpressure that never resumes.** If the app
  stops pulling the incoming-streams reader for a moment (e.g. while reading a
  stream's body), Firefox fills the reader's queue (~2) and then **never delivers
  any more streams, even after the app resumes pulling.**

  Evidence:
  - The decisive pair: `server-bi-probe-5` **passes** (it pulls all 5 stream
    objects back-to-back, *then* reads their bodies), while `server-bi-5`
    **fails** (it reads each body before pulling the next). Same server, same 5
    streams — only the pull cadence differs.
  - Server trace (`RUST_LOG=info,quinn_proto=trace`): Firefox grants
    `MaxStreams { dir: Bi, count: 102 }`, the server opens **all 5** without
    blocking (`open_bi() returned i=0..4 elapsed_ms=0`) and writes header+payload
    to each — then **retransmits streams 2-4 repeatedly** because Firefox never
    consumes them. The bytes are on the wire; Firefox just doesn't surface the
    streams to JS.
  - `server-uni-3` fails the same way with **no echo/writable involved** → rules
    out the stream's send side; it's purely the incoming-stream reader.
  - `server-bi-serial/10` passes (server opens one at a time → reader never backs
    up); `server-mix` 2 uni + 2 bidi passes (per stream-type, ≤ queue);
    `client-bi-open-*` pass (limit is only on **incoming** streams).
  - **Workaround (works today):** drain the incoming-streams reader *promptly*
    into your own queue and process bodies separately — never `await` per-stream
    work inline between `reader.read()` calls. (This is exactly why the idiomatic
    `for await (const s of incomingBidirectionalStreams) { await handle(s) }`
    triggers the bug.)
  - **Root cause (Firefox MOZ_LOG):** every layer delivers all 5 streams — neqo,
    HTTP-3, and the parent→content IPC — but the content-process
    `IncomingBidirectionalStreams` ReadableStream only enqueues 2: its `Pull`
    callback fires twice and is never re-invoked from the backlog. Full trace in
    [`firefox-bug/`](firefox-bug/README.md).

## Layout

It has two halves:

- `rs/web-transport-quinn/examples/test-server.rs` — a server that picks its
  behavior from the request **URL path** (e.g. `/server-bi/2`, `/server-close/42`).
- `js/web-demo/test.html` + `test.js` — a page that runs each scenario, records
  PASS / FAIL / TIMEOUT, and logs what happened.

## 1. Generate a local certificate

Browsers accept the self-signed cert via `serverCertificateHashes` (SHA-256
pinning), so no CA is needed. The cert is only valid for 10 days — regenerate if
connections start failing at `ready`.

```bash
bash dev/setup   # writes dev/localhost.{crt,key,hex}
```

## 2. Start the test server

> Port 4443 may already be taken on this machine (e.g. by `moq-relay`). Pick a
> free port and point the harness at it via the `server` box (or `?base=`).

```bash
cargo run --example test-server -p web-transport-quinn -- \
    --tls-cert dev/localhost.crt --tls-key dev/localhost.key \
    --addr 127.0.0.1:4444
```

The cert SAN covers `localhost` and `127.0.0.1`. Use `127.0.0.1` in the harness
URL so it matches the bind address (plain `localhost` may resolve to IPv6 `::1`).

## 3. Serve the harness

```bash
cd js/web-demo
bun install
npx parcel serve client.html test.html --port 1234
```

## 4. Run it in each browser

Open <http://localhost:1234/test.html?base=https://127.0.0.1:4444> in **Firefox**
and **Chrome**, then click **Run all** (or **Run** on a single row).

- **Yellow rows close the session.** If the tab dies ("Aww, Snap!") while one of
  these runs, *that scenario is the Chrome repro* — note its name.
- ⭐ / ⚠ mark the prime suspects.
- A hang shows up as **TIMEOUT** (default 8s, configurable) rather than freezing.

### Deep links (handy for filing bugs / automation)

- One scenario: `…/test.html?base=…&scenario=server-bi-2`
- Auto-run everything: `…/test.html?base=…&run=all`

## Scenarios

**⭐ = reproduces the Firefox bug** (server-initiated streams stall at 2).

| Scenario | URL path | What it exercises |
|---|---|---|
| `client-bi-echo` | `/client-bi-echo` | Baseline: client bidi → server echo |
| `datagram-echo` | `/datagram-echo` | Baseline: client datagram → server echo |
| `server-uni-1` | `/server-uni/1` | 1 server-initiated uni stream (passes) |
| **`server-uni-3`** ⭐ | `/server-uni/3` | 3 server-initiated uni streams — **Firefox surfaces only 2** |
| `server-bi-1` / `server-bi-2` | `/server-bi/N` | 1–2 server-initiated bidi streams (passes, at the limit) |
| **`server-bi-5`** ⭐ | `/server-bi/5` | 5 server-initiated bidi streams — **Firefox surfaces only 2** |
| **`server-bi-probe-5`** ⭐ | `/server-bi/5` | Probe: count stream OBJECTS delivered vs data flushed |
| **`server-bi-concurrent-3`** ⭐ | `/server-bi-concurrent/3` | 3 bidi opened concurrently — **stalls at 2, out of order** |
| `server-bi-serial-10` | `/server-bi-serial/10` | 10 bidi, drained one-at-a-time (passes → draining unblocks) |
| `server-bi-no-finish-2` | `/server-bi-no-finish/2` | 2 server bidi streams, never FIN'd |
| `server-mix-2uni-2bi` | `/server-mix/2` | 2 uni + 2 bidi (passes → limit is per-type, not shared) |
| `server-datagram-3` | `/server-datagram/3` | Server-sent datagrams |
| `client-bi-open-3/5/10` | `/echo` | **Client** opens N bidi streams (passes → direction matters) |
| `client-bi-open-concurrent-5` | `/echo` | Client opens 5 bidi streams concurrently (passes) |
| `client-uni-open-5/10` | `/echo` | **Client** opens N uni streams (passes) |
| `server-close-0` ⚠ | `/server-close/0` | Server closes session, code 0 |
| `server-close-42` ⚠ | `/server-close/42` | Server closes session, code 42 + reason |
| `server-close-immediate` ⚠ | `/server-close-immediate/7` | Server closes the instant it's accepted |
| `server-close-after-bi` ⚠ | `/server-close-after-bi/9` | Server opens a bidi stream, then closes |
| `server-close-after-echo` ⚠ | `/server-close-after-echo/3` | Echo once, then server closes |
| `client-close-immediate` ⚠ | `/echo` | Client closes right after connect |
| `client-close-after-echo` | `/client-bi-echo` | Echo once, then client closes |
| `reject-404/403/401/400/429/500/503` ⚠ | `/reject/<code>` | Server **rejects the CONNECT** with that HTTP status (`transport.ready` rejects) |
| `mixed` | `/mixed/0` | uni + bidi + datagram + echo, then close |

The `arg` (second path segment) is a **count** for the stream/datagram
scenarios, a **close code** for the close scenarios, and the **HTTP status code**
for `reject`.

## Adding a scenario

1. Add a `match` arm in `run_scenario()` in `test-server.rs`.
2. Add an entry to `SCENARIOS` in `test.js` with the matching `path` and the
   client steps. Set `noAutoClose: true` if the server (or the scenario itself)
   closes the session.
