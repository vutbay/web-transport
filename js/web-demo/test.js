// WebTransport browser regression harness.
//
// Pairs with `rs/web-transport-quinn/examples/test-server.rs`. Each scenario
// picks a server behavior via the URL path and runs the matching client steps,
// then reports PASS / FAIL / TIMEOUT. Open this in Firefox and Chrome and click
// "Run all" — the goal is to pin down:
//
//   * Firefox breaking on server-initiated bidirectional streams (esp. the 2nd).
//   * Chrome "Aww, Snap! Error code 11" (renderer crash) on explicit session close.
//
// A renderer crash kills the page, so those scenarios can't self-report — if the
// tab dies while a "⚠ Chrome crash suspect" scenario is running, THAT is the repro.

// @ts-expect-error embed the certificate fingerprint using bundler
import fingerprintHex from "bundle-text:../../dev/localhost.hex";

// Convert the hex fingerprint to bytes for serverCertificateHashes.
const fingerprint = [];
for (let c = 0; c < fingerprintHex.length - 1; c += 2) {
	fingerprint.push(parseInt(fingerprintHex.substring(c, c + 2), 16));
}

const enc = (s) => new TextEncoder().encode(s);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function assert(cond, msg) {
	if (!cond) throw new Error(msg || "assertion failed");
}

function options() {
	return {
		serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(fingerprint) }],
	};
}

// ----- low-level stream/datagram helpers -----------------------------------

// Read a ReadableStream of Uint8Array to completion as text (waits for FIN).
async function readAll(readable) {
	const reader = readable.getReader();
	const dec = new TextDecoder();
	let out = "";
	for (;;) {
		const { value, done } = await reader.read();
		if (done) break;
		out += dec.decode(value, { stream: true });
	}
	out += dec.decode();
	reader.releaseLock();
	return out;
}

// Accept `count` server-initiated bidi streams, reading each to FIN.
// Optionally echo the payload back so the server's recv side isn't reset.
async function readIncomingBidi(transport, count, log, echo = true) {
	const reader = transport.incomingBidirectionalStreams.getReader();
	const got = [];
	try {
		for (let i = 0; i < count; i++) {
			const { value: stream, done } = await reader.read();
			if (done) throw new Error(`incomingBidirectionalStreams ended after ${i}/${count}`);
			// Separate "stream object delivered" from "data flushed" so we can tell
			// a delivery cap (reader.read never yields #i) from a flush stall
			// (object arrives but readable never produces data).
			log(`accepted server bidi #${i} (object delivered; awaiting data)`);
			const text = await readAll(stream.readable);
			log(`  flushed server bidi #${i}: "${text}"`);
			got.push(text);
			if (echo) {
				const w = stream.writable.getWriter();
				await w.write(enc(`echo:${text}`));
				await w.close();
			}
		}
	} finally {
		reader.releaseLock();
	}
	return got;
}

// Like readIncomingBidi but the server never FINs — read only the first chunk.
async function readIncomingBidiNoFin(transport, count, log) {
	const reader = transport.incomingBidirectionalStreams.getReader();
	const got = [];
	try {
		for (let i = 0; i < count; i++) {
			const { value: stream, done } = await reader.read();
			if (done) throw new Error(`incomingBidirectionalStreams ended after ${i}/${count}`);
			const r = stream.readable.getReader();
			const { value } = await r.read();
			const text = new TextDecoder().decode(value);
			log(`recv server bidi (no-fin) #${i}: "${text}"`);
			got.push(text);
			r.cancel().catch(() => {});
		}
	} finally {
		reader.releaseLock();
	}
	return got;
}

// Accept `count` server-initiated unidirectional streams.
async function readIncomingUni(transport, count, log) {
	const reader = transport.incomingUnidirectionalStreams.getReader();
	const got = [];
	try {
		for (let i = 0; i < count; i++) {
			const { value: stream, done } = await reader.read();
			if (done) throw new Error(`incomingUnidirectionalStreams ended after ${i}/${count}`);
			log(`accepted server uni #${i} (object delivered; awaiting data)`);
			const text = await readAll(stream);
			log(`  flushed server uni #${i}: "${text}"`);
			got.push(text);
		}
	} finally {
		reader.releaseLock();
	}
	return got;
}

// Read `count` datagrams from the server.
async function readDatagrams(transport, count, log) {
	const reader = transport.datagrams.readable.getReader();
	const dec = new TextDecoder();
	const got = [];
	try {
		for (let i = 0; i < count; i++) {
			const { value, done } = await reader.read();
			if (done) throw new Error(`datagrams ended after ${i}/${count}`);
			const text = dec.decode(value);
			log(`recv server datagram #${i}: "${text}"`);
			got.push(text);
		}
	} finally {
		reader.releaseLock();
	}
	return got;
}

// Open `count` client-initiated bidi streams, echoing each. Tests whether the
// stall is direction-specific: client->server vs server->client. The server
// (echo loop) grants generous stream credit, so if Firefox stalls server-bi-N
// at 2 but sails through client-bi-open-N, the limit is on *incoming* streams.
async function openClientBidi(transport, count, log, concurrent = false) {
	const one = async (i) => {
		const s = await transport.createBidirectionalStream();
		const w = s.writable.getWriter();
		await w.write(enc(`client-bi-${i}`));
		await w.close();
		const text = await readAll(s.readable);
		log(`opened+echoed client bidi #${i}: "${text}"`);
		return text;
	};
	if (concurrent) return Promise.all(Array.from({ length: count }, (_, i) => one(i)));
	const got = [];
	for (let i = 0; i < count; i++) got.push(await one(i));
	return got;
}

// Open `count` client-initiated unidirectional streams (server drains them).
// `createUnidirectionalStream()` itself blocks if the peer's stream-count limit
// is hit, so awaiting each open detects a credit stall on the client->server
// direction.
async function openClientUni(transport, count, log) {
	const got = [];
	for (let i = 0; i < count; i++) {
		const stream = await transport.createUnidirectionalStream();
		const w = stream.getWriter();
		await w.write(enc(`client-uni-${i}`));
		await w.close();
		log(`opened client uni #${i}`);
		got.push(i);
	}
	return got;
}

// Two-phase probe: first collect stream OBJECTS from incomingBidirectionalStreams
// (without reading their data), then read each. Pinpoints the exact layer of the
// Firefox stall — how many stream objects are delivered vs how many flush data.
async function probeIncomingBidi(transport, count, log) {
	const reader = transport.incomingBidirectionalStreams.getReader();
	const streams = [];
	// Phase 1 — accept stream objects only.
	for (let i = 0; i < count; i++) {
		let res;
		try {
			res = await withTimeout(reader.read(), 3000, `accept #${i}`);
		} catch (e) {
			log(`accept #${i}: ${e.message}`);
			break;
		}
		if (res.done) {
			log(`incoming ended after ${i}`);
			break;
		}
		log(`accepted object #${i}`);
		streams.push(res.value);
	}
	log(`==> ${streams.length}/${count} stream OBJECTS delivered`);
	// Phase 2 — read data from each collected stream.
	let flushed = 0;
	for (let i = 0; i < streams.length; i++) {
		try {
			const text = await withTimeout(readAll(streams[i].readable), 3000, `read #${i}`);
			log(`data #${i}: "${text}"`);
			flushed++;
		} catch (e) {
			log(`data #${i}: ${e.message}`);
		}
	}
	log(`==> ${flushed}/${streams.length} streams FLUSHED data`);
	reader.releaseLock();
	return { objects: streams.length, flushed };
}

// Wait for the session to close, surfacing the close code/reason either way.
async function expectClosed(transport, log) {
	try {
		const info = await transport.closed;
		log(`transport.closed RESOLVED: closeCode=${info?.closeCode}, reason="${info?.reason}"`);
		return info ?? {};
	} catch (e) {
		log(`transport.closed REJECTED: ${e?.message ?? e}`);
		return { closeCode: e?.closeCode, reason: String(e?.message ?? e), rejected: true };
	}
}

// ----- scenarios -----------------------------------------------------------
//
// `kind`:
//   "baseline"  — basic sanity
//   "server"    — server-initiated streams/datagrams (Firefox suspects)
//   "close"     — explicit session close (Chrome crash suspects)
// `noAutoClose`: don't call transport.close() in cleanup (the scenario or server
//   already closes it).

const SCENARIOS = [
	{
		name: "client-bi-echo",
		path: "client-bi-echo",
		kind: "baseline",
		desc: "Baseline: client opens a bidi stream, server echoes.",
		async run(t, { log }) {
			const s = await t.createBidirectionalStream();
			const w = s.writable.getWriter();
			await w.write(enc("hello"));
			await w.close();
			const text = await readAll(s.readable);
			log(`echo: "${text}"`);
			assert(text === "hello", `expected "hello", got "${text}"`);
		},
	},
	{
		name: "datagram-echo",
		path: "datagram-echo",
		kind: "baseline",
		desc: "Baseline: client sends a datagram, server echoes.",
		async run(t, { log }) {
			const w = t.datagrams.writable.getWriter();
			await w.write(enc("ping"));
			w.releaseLock();
			const [d] = await readDatagrams(t, 1, log);
			assert(d === "ping", `expected "ping", got "${d}"`);
		},
	},
	{
		name: "server-uni-1",
		path: "server-uni/1",
		kind: "server",
		desc: "Server opens 1 unidirectional stream.",
		async run(t, { log }) {
			const g = await readIncomingUni(t, 1, log);
			assert(g[0] === "server-uni-0", `got "${g[0]}"`);
		},
	},
	{
		name: "server-uni-3",
		path: "server-uni/3",
		kind: "server",
		desc: "⭐ FAILS IN FIREFOX: server opens 3 uni streams; Firefox surfaces only 2.",
		async run(t, { log }) {
			const g = await readIncomingUni(t, 3, log);
			assert(g.length === 3, `got ${g.length}`);
		},
	},
	{
		name: "server-bi-1",
		path: "server-bi/1",
		kind: "server",
		desc: "Server opens 1 bidi stream (client echoes back).",
		async run(t, { log }) {
			const g = await readIncomingBidi(t, 1, log);
			assert(g[0] === "server-bi-0", `got "${g[0]}"`);
		},
	},
	{
		name: "server-bi-2",
		path: "server-bi/2",
		kind: "server",
		desc: "Server opens 2 bidi streams sequentially (passes — right at the limit).",
		async run(t, { log }) {
			const g = await readIncomingBidi(t, 2, log);
			assert(g.length === 2, `only got ${g.length}/2 streams`);
		},
	},
	{
		name: "server-bi-5",
		path: "server-bi/5",
		kind: "server",
		desc: "⭐ FAILS IN FIREFOX: server opens 5 bidi streams; Firefox surfaces only 2.",
		async run(t, { log }) {
			const g = await readIncomingBidi(t, 5, log);
			assert(g.length === 5, `only got ${g.length}/5 streams`);
		},
	},
	{
		name: "server-bi-probe-5",
		path: "server-bi/5",
		kind: "server",
		desc: "⭐ FAILS IN FIREFOX: probe — accept all 5 stream OBJECTS first, then read each (separates delivery cap from flush stall).",
		async run(t, { log }) {
			const r = await probeIncomingBidi(t, 5, log);
			assert(r.flushed === 5, `objects=${r.objects}/5, flushed=${r.flushed}/5`);
		},
	},
	{
		name: "server-mix-2uni-2bi",
		path: "server-mix/2",
		kind: "server",
		desc: "Server opens 2 uni + 2 bidi (passes in Firefox → the limit is per-type, not shared across types).",
		async run(t, { log }) {
			const [u, b] = await Promise.all([readIncomingUni(t, 2, log), readIncomingBidi(t, 2, log)]);
			assert(u.length === 2 && b.length === 2, `got uni=${u.length}/2 bi=${b.length}/2`);
		},
	},
	{
		name: "server-bi-serial-10",
		path: "server-bi-serial/10",
		kind: "server",
		desc: "Server opens 10 bidi but waits for the client to fully close each before the next (passes in Firefox → draining unblocks delivery).",
		async run(t, { log }) {
			const g = await readIncomingBidi(t, 10, log);
			assert(g.length === 10, `only got ${g.length}/10 streams`);
		},
	},
	{
		name: "server-bi-concurrent-3",
		path: "server-bi-concurrent/3",
		kind: "server",
		desc: "⭐ FAILS IN FIREFOX: server opens 3 bidi concurrently; Firefox surfaces only 2 (and out of order).",
		async run(t, { log }) {
			const g = await readIncomingBidi(t, 3, log);
			assert(g.length === 3, `only got ${g.length}/3 streams`);
		},
	},
	{
		name: "server-bi-no-finish-2",
		path: "server-bi-no-finish/2",
		kind: "server",
		desc: "Server opens 2 bidi streams but never FINs them.",
		async run(t, { log }) {
			const g = await readIncomingBidiNoFin(t, 2, log);
			assert(g.length === 2, `only got ${g.length}/2 streams`);
		},
	},
	{
		name: "server-datagram-3",
		path: "server-datagram/3",
		kind: "server",
		desc: "Server sends 3 datagrams.",
		async run(t, { log }) {
			const g = await readDatagrams(t, 3, log);
			assert(g.length === 3, `only got ${g.length}/3 datagrams`);
		},
	},
	// ----- client-initiated streams (does direction/endpoint matter?) ------
	//
	// Compare against server-bi-N / server-uni-N. If Firefox stalls when the
	// SERVER opens streams but not when the CLIENT does, the limit is on
	// incoming (server->client) stream credit specifically.
	{
		name: "client-bi-open-3",
		path: "echo",
		kind: "server",
		desc: "Client opens 3 bidi streams (server echoes each).",
		async run(t, { log }) {
			const g = await openClientBidi(t, 3, log);
			assert(g.length === 3, `only ${g.length}/3`);
		},
	},
	{
		name: "client-bi-open-5",
		path: "echo",
		kind: "server",
		desc: "Client opens 5 bidi streams (passes — client-initiated direction is unaffected).",
		async run(t, { log }) {
			const g = await openClientBidi(t, 5, log);
			assert(g.length === 5, `only ${g.length}/5`);
		},
	},
	{
		name: "client-bi-open-10",
		path: "echo",
		kind: "server",
		desc: "Client opens 10 bidi streams sequentially.",
		async run(t, { log }) {
			const g = await openClientBidi(t, 10, log);
			assert(g.length === 10, `only ${g.length}/10`);
		},
	},
	{
		name: "client-bi-open-concurrent-5",
		path: "echo",
		kind: "server",
		desc: "Client opens 5 bidi streams concurrently.",
		async run(t, { log }) {
			const g = await openClientBidi(t, 5, log, true);
			assert(g.length === 5, `only ${g.length}/5`);
		},
	},
	{
		name: "client-uni-open-5",
		path: "echo",
		kind: "server",
		desc: "Client opens 5 uni streams (server drains). Compare with server-uni-3.",
		async run(t, { log }) {
			const g = await openClientUni(t, 5, log);
			assert(g.length === 5, `only ${g.length}/5`);
			// Confirm the session is still usable afterwards.
			const s = await t.createBidirectionalStream();
			const w = s.writable.getWriter();
			await w.write(enc("alive?"));
			await w.close();
			log(`alive check echo: "${await readAll(s.readable)}"`);
		},
	},
	{
		name: "client-uni-open-10",
		path: "echo",
		kind: "server",
		desc: "Client opens 10 uni streams sequentially.",
		async run(t, { log }) {
			const g = await openClientUni(t, 10, log);
			assert(g.length === 10, `only ${g.length}/10`);
		},
	},
	{
		name: "server-close-0",
		path: "server-close/0",
		kind: "close",
		noAutoClose: true,
		desc: "⚠ Server closes the session with code 0. Chrome crash suspect.",
		async run(t, { log }) {
			const info = await expectClosed(t, log);
			assert(info.closeCode === 0, `expected closeCode 0, got ${info.closeCode}`);
		},
	},
	{
		name: "server-close-42",
		path: "server-close/42",
		kind: "close",
		noAutoClose: true,
		desc: "⚠ Server closes the session with code 42 + reason. Chrome crash suspect.",
		async run(t, { log }) {
			const info = await expectClosed(t, log);
			assert(info.closeCode === 42, `expected closeCode 42, got ${info.closeCode}`);
		},
	},
	{
		name: "server-close-immediate",
		path: "server-close-immediate/7",
		kind: "close",
		noAutoClose: true,
		desc: "⚠ Server closes the instant the session is accepted (races ready). Chrome crash suspect.",
		async run(t, { log }) {
			await expectClosed(t, log);
		},
	},
	{
		name: "server-close-after-bi",
		path: "server-close-after-bi/9",
		kind: "close",
		noAutoClose: true,
		desc: "⚠ Server opens a bidi stream, then closes the session. Chrome crash suspect.",
		async run(t, { log }) {
			await readIncomingBidi(t, 1, log, false).catch((e) => log(`(stream read: ${e.message})`));
			await expectClosed(t, log);
		},
	},
	{
		name: "server-close-after-echo",
		path: "server-close-after-echo/3",
		kind: "close",
		noAutoClose: true,
		desc: "⚠ Client opens bidi, server echoes then closes the session. Chrome crash suspect.",
		async run(t, { log }) {
			const s = await t.createBidirectionalStream();
			const w = s.writable.getWriter();
			await w.write(enc("bye"));
			await w.close();
			const text = await readAll(s.readable);
			log(`echo: "${text}"`);
			await expectClosed(t, log);
		},
	},
	{
		name: "client-close-immediate",
		path: "echo",
		kind: "close",
		noAutoClose: true,
		desc: "⚠ Client connects then immediately closes. Chrome crash suspect (connect/disconnect).",
		async run(t, { log }) {
			t.close({ closeCode: 5, reason: "client-bye" });
			log("client called close({closeCode:5})");
			await sleep(200);
		},
	},
	{
		name: "client-close-after-echo",
		path: "client-bi-echo",
		kind: "close",
		noAutoClose: true,
		desc: "Client echoes once then closes (the original demo flow).",
		async run(t, { log }) {
			const s = await t.createBidirectionalStream();
			const w = s.writable.getWriter();
			await w.write(enc("hello"));
			await w.close();
			const text = await readAll(s.readable);
			log(`echo: "${text}"`);
			t.close();
			log("client called close()");
			await sleep(200);
		},
	},
	// ----- rejected CONNECT (non-200 status) -------------------------------
	//
	// The server replies to the CONNECT with an HTTP error instead of accepting,
	// so the session is never established and `transport.ready` rejects. This is
	// a different code path from "accept then close" and a suspected Chrome crash
	// trigger. `skipReady` tells the runner not to await ready itself — the
	// scenario does it and expects a rejection.
	...[
		{ code: 404, name: "reject-404" },
		{ code: 403, name: "reject-403" },
		{ code: 401, name: "reject-401" },
		{ code: 400, name: "reject-400" },
		{ code: 429, name: "reject-429" },
		{ code: 500, name: "reject-500" },
		{ code: 503, name: "reject-503" },
	].map(({ code, name }) => ({
		name,
		path: `reject/${code}`,
		kind: "close",
		noAutoClose: true,
		skipReady: true,
		desc: `⚠ Server rejects the CONNECT with HTTP ${code}. Suspected Chrome crash trigger.`,
		async run(t, { log }) {
			try {
				await t.ready;
				throw new Error("transport.ready RESOLVED but server should have rejected");
			} catch (e) {
				log(`ready REJECTED (expected): ${e?.name}: ${e?.message ?? e}`);
			}
			await t.closed
				.then((i) => log(`closed RESOLVED: ${JSON.stringify(i)}`))
				.catch((e) => log(`closed REJECTED: ${e?.name}: ${e?.message ?? e}`));
		},
	})),
	{
		name: "mixed",
		path: "mixed/0",
		kind: "close",
		noAutoClose: true,
		desc: "Kitchen sink: server uni + bidi + datagram + echo, then close.",
		async run(t, { log }) {
			const uni = readIncomingUni(t, 1, log).catch((e) => log(`uni: ${e.message}`));
			const bi = readIncomingBidi(t, 1, log, false).catch((e) => log(`bidi: ${e.message}`));
			const dg = readDatagrams(t, 1, log).catch((e) => log(`dgram: ${e.message}`));
			const s = await t.createBidirectionalStream();
			const w = s.writable.getWriter();
			await w.write(enc("kitchen"));
			await w.close();
			await readAll(s.readable)
				.then((x) => log(`echo: "${x}"`))
				.catch((e) => log(`echo: ${e.message}`));
			await Promise.allSettled([uni, bi, dg]);
			await expectClosed(t, log);
		},
	},
];

// ----- runner --------------------------------------------------------------

function withTimeout(p, ms, label) {
	let timer;
	const timeout = new Promise((_, reject) => {
		timer = setTimeout(() => {
			const e = new Error(`timeout after ${ms}ms (${label})`);
			e.__timeout = true;
			reject(e);
		}, ms);
	});
	return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
}

const ui = {
	base: () => document.getElementById("base").value.replace(/\/$/, ""),
	timeout: () => parseInt(document.getElementById("timeout").value, 10) || 8000,
	row: (name) => document.getElementById(`row-${name}`),
};

function setStatus(name, status) {
	const cell = ui.row(name).querySelector(".status");
	cell.textContent = status;
	cell.className = `status ${status.toLowerCase()}`;
}

function appendLog(name, msg) {
	const pre = ui.row(name).querySelector(".log");
	pre.textContent += (pre.textContent ? "\n" : "") + msg;
	console.log(`[${name}] ${msg}`);
}

function clearLog(name) {
	ui.row(name).querySelector(".log").textContent = "";
}

async function runScenario(sc) {
	clearLog(sc.name);
	setStatus(sc.name, "RUNNING");
	const log = (m) => appendLog(sc.name, m);
	const url = `${ui.base()}/${sc.path}`;
	const ms = ui.timeout();
	log(`connecting ${url}`);

	let transport;
	try {
		transport = new WebTransport(url, options());
	} catch (e) {
		setStatus(sc.name, "FAIL");
		log(`WebTransport constructor threw: ${e}`);
		return "FAIL";
	}

	try {
		if (!sc.skipReady) {
			await withTimeout(transport.ready, ms, "ready");
			log("ready");
		}
		await withTimeout(sc.run(transport, { log }), ms, sc.name);
		setStatus(sc.name, "PASS");
		return "PASS";
	} catch (e) {
		const kind = e?.__timeout ? "TIMEOUT" : "FAIL";
		setStatus(sc.name, kind);
		log(`${kind}: ${e?.message ?? e}`);
		return kind;
	} finally {
		if (!sc.noAutoClose) {
			try {
				transport?.close();
			} catch {}
		}
	}
}

async function runAll() {
	const summary = { PASS: 0, FAIL: 0, TIMEOUT: 0 };
	for (const sc of SCENARIOS) {
		const result = await runScenario(sc);
		summary[result] = (summary[result] || 0) + 1;
		updateSummary(summary);
		await sleep(300); // let the previous session fully tear down
	}
}

function updateSummary(summary) {
	document.getElementById("summary").textContent =
		`PASS ${summary.PASS || 0} · FAIL ${summary.FAIL || 0} · TIMEOUT ${summary.TIMEOUT || 0}`;
}

// ----- DOM bootstrap -------------------------------------------------------

function build() {
	document.getElementById("ua").textContent = navigator.userAgent;

	const tbody = document.getElementById("scenarios");
	for (const sc of SCENARIOS) {
		const tr = document.createElement("tr");
		tr.id = `row-${sc.name}`;
		tr.dataset.kind = sc.kind;
		tr.innerHTML = `
			<td><button class="run">Run</button></td>
			<td class="status pending">PENDING</td>
			<td class="name">${sc.name}<div class="desc">${sc.desc}</div></td>
			<td><pre class="log"></pre></td>
		`;
		tr.querySelector(".run").addEventListener("click", () => runScenario(sc));
		tbody.appendChild(tr);
	}

	document.getElementById("run-all").addEventListener("click", runAll);

	// Optional deep-linking: ?run=all, or ?scenario=server-bi-2
	const params = new URLSearchParams(window.location.search);
	if (params.get("base")) document.getElementById("base").value = params.get("base");
	const only = params.get("scenario");
	if (only) {
		const sc = SCENARIOS.find((s) => s.name === only);
		if (sc) runScenario(sc);
	} else if (params.get("run") === "all") {
		runAll();
	}
}

build();
