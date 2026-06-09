//! Scenario-driven WebTransport server for browser interop / regression testing.
//!
//! The client selects a scenario via the URL *path*, e.g.
//!
//!   https://localhost:4443/server-bi/2
//!   https://localhost:4443/server-close/42
//!
//! The first path segment is the scenario name, the (optional) second segment is
//! a numeric argument (a count, or a close code). This pairs with the browser
//! harness in `js/web-demo/test.html`, which knows the matching client behavior
//! for each scenario.
//!
//! Run it with:
//!
//!   cargo run --example test-server -p web-transport-quinn -- \
//!       --tls-cert dev/localhost.crt --tls-key dev/localhost.key
//!
//! Then open the harness (see js/web-demo) in Firefox and Chrome.

use std::{
    fs, io, path,
    time::{Duration, Instant},
};

use anyhow::Context;
use bytes::Bytes;
use clap::Parser;
use rustls::pki_types::CertificateDer;
use web_transport_quinn::{http, Session};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "[::]:4443")]
    addr: std::net::SocketAddr,

    /// Use the certificates at this path, encoded as PEM.
    #[arg(long)]
    pub tls_cert: path::PathBuf,

    /// Use the private key at this path, encoded as PEM.
    #[arg(long)]
    pub tls_key: path::PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Read the PEM certificate chain.
    let chain = fs::File::open(&args.tls_cert).context("failed to open cert file")?;
    let chain: Vec<CertificateDer> = rustls_pemfile::certs(&mut io::BufReader::new(chain))
        .collect::<Result<_, _>>()
        .context("failed to load certs")?;
    anyhow::ensure!(!chain.is_empty(), "could not find certificate");

    // Read the PEM private key.
    let keys = fs::File::open(&args.tls_key).context("failed to open key file")?;
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(keys))
        .context("failed to load private key")?
        .context("missing private key")?;

    let mut server = web_transport_quinn::ServerBuilder::new()
        .with_addr(args.addr)
        .with_certificate(chain, key)?;

    tracing::info!(addr = %args.addr, "listening; pick a scenario via the URL path");

    while let Some(conn) = server.accept().await {
        tokio::spawn(async move {
            if let Err(err) = run_conn(conn).await {
                tracing::error!(?err, "connection failed")
            }
        });
    }

    Ok(())
}

async fn run_conn(request: web_transport_quinn::Request) -> anyhow::Result<()> {
    // The scenario name is the first path segment, with an optional numeric arg.
    let path = request.url.path().to_string();
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    let scenario = segments.next().unwrap_or("echo").to_string();
    let arg: u64 = segments.next().and_then(|s| s.parse().ok()).unwrap_or(0);

    tracing::info!(url = %request.url, %scenario, arg, "received WebTransport request");

    // Reject the CONNECT with an HTTP status instead of accepting. `arg` is the
    // status code (default 404). This is a distinct path from a 200 + later
    // close — the session is never established, so the browser sees `ready`
    // reject. Suspected Chrome crash trigger.
    if scenario == "reject" {
        let status = u16::try_from(arg)
            .ok()
            .and_then(|c| http::StatusCode::from_u16(c).ok())
            .unwrap_or(http::StatusCode::NOT_FOUND);
        tracing::info!(%status, "rejecting session");
        request
            .reject(status)
            .await
            .context("failed to reject session")?;
        return Ok(());
    }

    let session = request.ok().await.context("failed to accept session")?;
    tracing::info!(%scenario, "accepted session");

    let res = run_scenario(&scenario, arg, session.clone()).await;
    match &res {
        Ok(()) => tracing::info!(%scenario, "scenario finished"),
        Err(err) => tracing::info!(%scenario, ?err, "scenario ended with error"),
    }

    Ok(())
}

async fn run_scenario(scenario: &str, arg: u64, session: Session) -> anyhow::Result<()> {
    match scenario {
        // ---- Baselines (client-driven) --------------------------------------
        //
        // Echo whatever the client sends, forever. Used as a sanity check that
        // the basic path works before blaming server-initiated streams.
        "echo" | "client-bi-echo" | "datagram-echo" => echo_loop(session).await,

        // ---- Server-initiated streams (Firefox suspects) --------------------
        //
        // Open N bidirectional streams from the server. `arg` is the count
        // (default 1). This is the prime suspect: "the second server initiated
        // bidirectional stream broke it last time" => /server-bi/2.
        "server-bi" => server_bi(session, arg.max(1) as usize, true, false).await,

        // Same, but never FIN the send side — some stacks treat an open-ended
        // server stream differently.
        "server-bi-no-finish" => server_bi(session, arg.max(1) as usize, false, false).await,

        // Open N bidi streams but WAIT for each to fully close (client FIN on its
        // send side) before opening the next. Tests whether closing a stream
        // makes the peer replenish stream credit (MAX_STREAMS).
        "server-bi-serial" => server_bi(session, arg.max(1) as usize, true, true).await,

        // Open N bidirectional streams *concurrently* (all before any is drained)
        // to stress ordering / flow-control on the client.
        "server-bi-concurrent" => server_bi_concurrent(session, arg.max(1) as usize).await,

        // Open N unidirectional streams from the server.
        "server-uni" => server_uni(session, arg.max(1) as usize).await,

        // Send N datagrams from the server.
        "server-datagram" => server_datagram(session, arg.max(1) as usize).await,

        // Open `arg` uni AND `arg` bidi streams, interleaved. Distinguishes a
        // per-type stream-credit limit (2 uni + 2 bidi both succeed) from a
        // shared one (stalls on the 3rd stream regardless of type).
        "server-mix" => server_mix(session, arg.max(1) as usize).await,

        // ---- Explicit session close (Chrome "Aww snap, code 11" suspects) ---
        //
        // Accept the session, then immediately close it with `arg` as the code.
        "server-close" => {
            tokio::time::sleep(Duration::from_millis(50)).await;
            session.close(arg as u32, b"server-close");
            session.closed().await;
            Ok(())
        }

        // Open one bidi stream, send data, then close the session with `arg` as
        // the code while the stream is still "live" on the client.
        "server-close-after-bi" => {
            let (mut send, _recv) = session.open_bi().await?;
            send.write_all(b"server-bi-0").await.ok();
            send.finish().ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
            session.close(arg as u32, b"server-close-after-bi");
            session.closed().await;
            Ok(())
        }

        // Close the session the instant it's accepted, no delay — races the
        // client's `ready`/first-stream against the close capsule.
        "server-close-immediate" => {
            session.close(arg as u32, b"server-close-immediate");
            session.closed().await;
            Ok(())
        }

        // Wait for the client to open a bidi stream, echo once, then the server
        // closes the session explicitly. Mirrors "closes during a live stream".
        "server-close-after-echo" => {
            if let Ok((mut send, mut recv)) = session.accept_bi().await {
                let msg = recv.read_to_end(1024).await.unwrap_or_default();
                send.write_all(&msg).await.ok();
                send.finish().ok();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            session.close(arg as u32, b"server-close-after-echo");
            session.closed().await;
            Ok(())
        }

        // ---- Kitchen sink ---------------------------------------------------
        //
        // Server opens a uni, a bidi, sends a datagram, echoes a client stream,
        // then closes. Exercises everything in one session.
        "mixed" => {
            let mut uni = session.open_uni().await?;
            uni.write_all(b"server-uni-0").await.ok();
            uni.finish().ok();

            let (mut send, recv) = session.open_bi().await?;
            send.write_all(b"server-bi-0").await.ok();
            send.finish().ok();
            tokio::spawn(drain(recv));

            session
                .send_datagram(Bytes::from_static(b"server-dgram-0"))
                .ok();

            if let Ok((mut s, mut r)) = session.accept_bi().await {
                let msg = r.read_to_end(1024).await.unwrap_or_default();
                s.write_all(&msg).await.ok();
                s.finish().ok();
            }

            session.close(arg as u32, b"mixed-done");
            session.closed().await;
            Ok(())
        }

        // Unknown scenario: log and fall back to echo so the client still gets
        // *something* rather than a silent hang.
        other => {
            tracing::warn!(scenario = %other, "unknown scenario, falling back to echo");
            echo_loop(session).await
        }
    }
}

/// Echo client-initiated streams (bidi echoed, uni drained) and datagrams until
/// the session closes. Used by both the baseline and the client-opens-N
/// scenarios, so it must accept all stream types the client might open.
async fn echo_loop(session: Session) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            res = session.accept_bi() => {
                let (mut send, mut recv) = res?;
                let msg = recv.read_to_end(1024).await?;
                tracing::info!(msg = %String::from_utf8_lossy(&msg), "echo bidi");
                send.write_all(&msg).await.ok();
                send.finish().ok();
            },
            res = session.accept_uni() => {
                let mut recv = res?;
                let msg = recv.read_to_end(1024).await?;
                tracing::info!(msg = %String::from_utf8_lossy(&msg), "drain uni");
            },
            res = session.read_datagram() => {
                let msg = res?;
                tracing::info!(msg = %String::from_utf8_lossy(&msg), "echo datagram");
                session.send_datagram(msg).ok();
            },
        }
    }
}

/// Open `count` server-initiated bidi streams, one after another.
///
/// `wait_close`: wait for the client to fully close each stream (FIN on its send
/// side) before opening the next. The per-open timing log makes a stream-credit
/// stall obvious — a blocked `open_bi()` shows up as a multi-second `elapsed_ms`.
async fn server_bi(
    session: Session,
    count: usize,
    finish: bool,
    wait_close: bool,
) -> anyhow::Result<()> {
    for i in 0..count {
        let start = Instant::now();
        let (mut send, mut recv) = session.open_bi().await?;
        tracing::info!(
            i,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "open_bi() returned"
        );

        let msg = format!("server-bi-{i}");
        send.write_all(msg.as_bytes()).await?;
        if finish {
            send.finish().ok();
        }

        if wait_close {
            // Block until the client finishes its send side = stream fully closed.
            let echo = recv.read_to_end(1024).await;
            tracing::info!(i, ok = echo.is_ok(), "client closed bidi stream");
        } else {
            // Drain the client's echo (if any) so the recv side isn't reset.
            tokio::spawn(drain(recv));
        }
    }

    // Keep the session alive so the client controls teardown.
    session.closed().await;
    Ok(())
}

/// Open `count` server-initiated bidi streams without awaiting between them.
async fn server_bi_concurrent(session: Session, count: usize) -> anyhow::Result<()> {
    let mut tasks = Vec::new();
    for i in 0..count {
        let session = session.clone();
        tasks.push(tokio::spawn(async move {
            let (mut send, recv) = session.open_bi().await?;
            let msg = format!("server-bi-{i}");
            send.write_all(msg.as_bytes()).await?;
            send.finish().ok();
            tracing::info!(i, "opened concurrent server bidi stream");
            drain(recv).await;
            Ok::<_, anyhow::Error>(())
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    session.closed().await;
    Ok(())
}

/// Open `count` uni and `count` bidi streams, interleaved (uni, bidi, uni, …).
/// The per-open timing log shows exactly which stream (and type) blocks.
async fn server_mix(session: Session, count: usize) -> anyhow::Result<()> {
    for i in 0..count {
        let start = Instant::now();
        let mut uni = session.open_uni().await?;
        tracing::info!(
            i,
            kind = "uni",
            elapsed_ms = start.elapsed().as_millis() as u64,
            "open returned"
        );
        uni.write_all(format!("server-uni-{i}").as_bytes()).await?;
        uni.finish().ok();

        let start = Instant::now();
        let (mut send, recv) = session.open_bi().await?;
        tracing::info!(
            i,
            kind = "bi",
            elapsed_ms = start.elapsed().as_millis() as u64,
            "open returned"
        );
        send.write_all(format!("server-bi-{i}").as_bytes()).await?;
        send.finish().ok();
        tokio::spawn(drain(recv));
    }

    session.closed().await;
    Ok(())
}

/// Open `count` server-initiated unidirectional streams.
async fn server_uni(session: Session, count: usize) -> anyhow::Result<()> {
    for i in 0..count {
        let start = Instant::now();
        let mut send = session.open_uni().await?;
        tracing::info!(
            i,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "open_uni() returned"
        );
        let msg = format!("server-uni-{i}");
        send.write_all(msg.as_bytes()).await?;
        send.finish().ok();
    }
    session.closed().await;
    Ok(())
}

/// Send `count` datagrams from the server.
async fn server_datagram(session: Session, count: usize) -> anyhow::Result<()> {
    for i in 0..count {
        let msg = format!("server-dgram-{i}");
        session.send_datagram(Bytes::from(msg)).ok();
        tracing::info!(i, "sent server datagram");
        // Space them out slightly so they aren't coalesced/dropped.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    session.closed().await;
    Ok(())
}

/// Read and discard a recv stream to completion, ignoring errors.
async fn drain(mut recv: web_transport_quinn::RecvStream) {
    let _ = recv.read_to_end(64 * 1024).await;
}
