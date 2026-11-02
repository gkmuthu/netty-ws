mod config;
mod proto;

use crate::config::Config;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};

const OPEN_FAIL_LOGS: u64 = 8;

struct Stats {
    connected: AtomicU64,
    inflight: AtomicU64,
    failed: AtomicU64,
    closed: AtomicU64,
    received: AtomicU64,
    target: u64,
}

impl Stats {
    fn new(target: u64) -> Self {
        Self {
            connected: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            closed: AtomicU64::new(0),
            received: AtomicU64::new(0),
            target,
        }
    }

    fn report(&self) -> String {
        format!(
            "{{\"connected\":{},\"inflight\":{},\"failed\":{},\"closed\":{},\"received\":{},\"target\":{}}}",
            self.connected.load(Ordering::Relaxed),
            self.inflight.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.closed.load(Ordering::Relaxed),
            self.received.load(Ordering::Relaxed),
            self.target
        )
    }

    fn scrape(&self) -> String {
        format!(
            "# TYPE ws_client_connected gauge\nws_client_connected {connected}\n# TYPE ws_client_target gauge\nws_client_target {target}\n# TYPE ws_client_inflight gauge\nws_client_inflight {inflight}\n# TYPE ws_client_failed_total counter\nws_client_failed_total {failed}\n# TYPE ws_client_closed_total counter\nws_client_closed_total {closed}\n# TYPE ws_client_messages_received_total counter\nws_client_messages_received_total {received}\n",
            connected = self.connected.load(Ordering::Relaxed),
            target = self.target,
            inflight = self.inflight.load(Ordering::Relaxed),
            failed = self.failed.load(Ordering::Relaxed),
            closed = self.closed.load(Ordering::Relaxed),
            received = self.received.load(Ordering::Relaxed)
        )
    }
}

fn main() {
    let config = match Config::from_args() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("ws-load-client: {err}");
            std::process::exit(2);
        }
    };
    println!("WebSocket load client {}", config.summary());
    let workers = config.workers;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .thread_name("ws-load")
        .build()
        .unwrap_or_else(|err| {
            eprintln!("ws-load-client: runtime: {err}");
            std::process::exit(2);
        });
    runtime.block_on(run(config));
    std::process::exit(0);
}

async fn run(config: Config) {
    let stats = Arc::new(Stats::new(config.connections as u64));
    let remote = match resolve_remote(&config).await {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("ws-load-client: {err}");
            std::process::exit(2);
        }
    };
    if config.metrics_port > 0 {
        let stats = Arc::clone(&stats);
        let port = config.metrics_port;
        tokio::spawn(async move {
            if let Err(err) = serve_metrics(port, stats).await {
                eprintln!("ws-load-client: metrics: {err}");
            }
        });
    }
    let report_stats = Arc::clone(&stats);
    let report_every = config.report_seconds;
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(report_every));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            println!("{}", report_stats.report());
        }
    });
    if config.initial_delay_seconds > 0 {
        println!("Waiting initial delay of {}s before connecting...", config.initial_delay_seconds);
        sleep(Duration::from_secs(config.initial_delay_seconds)).await;
    }
    let hold = config.hold_seconds;
    let payload = config.payload().map(Arc::new);
    let target = config.connections as u64;
    let config = Arc::new(config);
    if hold == 0 {
        println!("Running indefinitely until stopped (press Ctrl+C to terminate)...");
        tokio::select! {
            _ = ramp(Arc::clone(&config), Arc::clone(&stats), remote, payload) => {}
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutdown signal received. Stopping client...");
            }
        }
    } else {
        let ramp_stats = Arc::clone(&stats);
        tokio::select! {
            _ = ramp(Arc::clone(&config), Arc::clone(&stats), remote, payload) => {}
            _ = async {
                while ramp_stats.connected.load(Ordering::Relaxed) < target {
                    sleep(Duration::from_millis(100)).await;
                }
                println!("Target count {} reached. Holding for {}s...", target, hold);
                sleep(Duration::from_secs(hold)).await;
            } => {}
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutdown signal received. Stopping client early...");
            }
        }
    }
    println!("{}", stats.report());
}

async fn resolve_remote(config: &Config) -> Result<SocketAddr, String> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((config.host.as_str(), config.port))
        .await
        .map_err(|err| format!("cannot resolve {}:{}: {err}", config.host, config.port))?
        .collect();
    let prefer_v4 = config.local_addresses.iter().any(|addr| addr.is_ipv4()) || config.local_addresses.is_empty();
    addrs
        .iter()
        .copied()
        .find(|addr| !prefer_v4 || addr.is_ipv4())
        .or_else(|| addrs.first().copied())
        .ok_or_else(|| format!("no address for {}", config.host))
}

async fn ramp(config: Arc<Config>, stats: Arc<Stats>, remote: SocketAddr, payload: Option<Arc<Vec<u8>>>) {
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let cursor = AtomicU64::new(0);
    loop {
        tick.tick().await;
        let connected = stats.connected.load(Ordering::Relaxed);
        let inflight = stats.inflight.load(Ordering::Relaxed);
        let occupied = connected.saturating_add(inflight).min(config.connections as u64);
        let room = config.connections as u64 - occupied;
        let batch = (config.ramp_per_second as u64).min(room);
        for _ in 0..batch {
            stats.inflight.fetch_add(1, Ordering::Relaxed);
            let local = next_local(&config, &cursor);
            let config = Arc::clone(&config);
            let stats = Arc::clone(&stats);
            let payload = payload.clone();
            tokio::spawn(async move {
                session(config, stats, remote, local, payload).await;
            });
        }
    }
}

fn next_local(config: &Config, cursor: &AtomicU64) -> Option<SocketAddr> {
    if config.local_addresses.is_empty() {
        return None;
    }
    let index = cursor.fetch_add(1, Ordering::Relaxed) as usize % config.local_addresses.len();
    Some(config.local_addresses[index])
}

async fn session(
    config: Arc<Config>,
    stats: Arc<Stats>,
    remote: SocketAddr,
    local: Option<SocketAddr>,
    payload: Option<Arc<Vec<u8>>>,
) {
    if let Err(err) = connect_and_hold(&config, &stats, remote, local, payload).await {
        let n = stats.failed.fetch_add(1, Ordering::Relaxed) + 1;
        if n <= OPEN_FAIL_LOGS {
            eprintln!("ws-load-client: connection failure: {err}");
        }
    }
}

async fn connect_and_hold(
    config: &Config,
    stats: &Stats,
    remote: SocketAddr,
    local: Option<SocketAddr>,
    payload: Option<Arc<Vec<u8>>>,
) -> Result<(), String> {
    let mut stream = match connect_socket(config, remote, local).await {
        Ok(s) => s,
        Err(e) => {
            stats.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(e);
        }
    };
    let mut pending = match handshake(config, &mut stream).await {
        Ok(p) => p,
        Err(e) => {
            stats.inflight.fetch_sub(1, Ordering::Relaxed);
            return Err(e);
        }
    };
    stats.inflight.fetch_sub(1, Ordering::Relaxed);
    stats.connected.fetch_add(1, Ordering::Relaxed);
    let res = hold_session(config, stats, &mut stream, &mut pending, payload).await;
    stats.connected.fetch_sub(1, Ordering::Relaxed);
    stats.closed.fetch_add(1, Ordering::Relaxed);
    res
}

async fn connect_socket(config: &Config, remote: SocketAddr, local: Option<SocketAddr>) -> Result<TcpStream, String> {
    if let Some(local) = local {
        if local.is_ipv4() != remote.is_ipv4() {
            return Err("local address family does not match target".into());
        }
    }
    timeout(Duration::from_secs(5), async move {
        let socket = if remote.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(|err| err.to_string())?;
        if config.recv_buffer > 0 {
            socket
                .set_recv_buffer_size(config.recv_buffer as u32)
                .map_err(|err| err.to_string())?;
        }
        if config.send_buffer > 0 {
            socket
                .set_send_buffer_size(config.send_buffer as u32)
                .map_err(|err| err.to_string())?;
        }
        if let Some(local) = local {
            socket.bind(local).map_err(|err| err.to_string())?;
        }
        let stream = socket.connect(remote).await.map_err(|err| err.to_string())?;
        stream.set_nodelay(true).map_err(|err| err.to_string())?;
        Ok(stream)
    })
    .await
    .map_err(|_| "connect timeout".to_string())?
}

async fn handshake(config: &Config, stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let (key, accept) = proto::websocket_key();
    let request = proto::handshake_request(config, &key);
    timeout(Duration::from_secs(5), async {
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|err| err.to_string())?;
        let mut buf = Vec::with_capacity(256);
        let mut tmp = [0u8; 256];
        loop {
            if buf.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
            if buf.len() > 8192 {
                return Err("handshake larger than 8192 bytes".into());
            }
            let n = stream.read(&mut tmp).await.map_err(|err| err.to_string())?;
            if n == 0 {
                return Err("connection closed during handshake".into());
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        let header_end = buf
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or_else(|| "incomplete handshake".to_string())?;
        let head = String::from_utf8_lossy(&buf[..header_end]);
        proto::handshake_ok(&head, &accept)?;
        Ok(buf.split_off(header_end + 4))
    })
    .await
    .map_err(|_| "handshake timeout".to_string())?
}

async fn hold_session(
    config: &Config,
    stats: &Stats,
    stream: &mut TcpStream,
    pending: &mut Vec<u8>,
    payload: Option<Arc<Vec<u8>>>,
) -> Result<(), String> {
    let mut ping = interval(Duration::from_secs(config.ping_seconds.max(1)));
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping.tick().await;
    let mut messages = interval(Duration::from_secs(config.message_interval_seconds.max(1)));
    messages.set_missed_tick_behavior(MissedTickBehavior::Delay);
    messages.tick().await;
    if let Some(body) = payload.as_ref() {
        stream
            .write_all(&proto::encode_frame(0x1, body))
            .await
            .map_err(|err| err.to_string())?;
    }
    let mut tmp = [0u8; 256];
    loop {
        tokio::select! {
            read = stream.read(&mut tmp) => {
                let n = read.map_err(|err| err.to_string())?;
                if n == 0 {
                    return Ok(());
                }
                pending.extend_from_slice(&tmp[..n]);
                while let Some(frame) = proto::pop_frame(pending)? {
                    handle_frame(stream, stats, frame).await?;
                }
            }
            _ = ping.tick(), if config.ping_seconds > 0 => {
                stream.write_all(&proto::encode_frame(0x9, b"")).await.map_err(|err| err.to_string())?;
            }
            _ = messages.tick(), if payload.is_some() => {
                let body = payload.as_ref().unwrap();
                stream.write_all(&proto::encode_frame(0x1, body)).await.map_err(|err| err.to_string())?;
            }
        }
    }
}

async fn handle_frame(stream: &mut TcpStream, stats: &Stats, frame: proto::Frame) -> Result<(), String> {
    match frame.opcode {
        0x1 | 0x2 if frame.fin => {
            stats.received.fetch_add(1, Ordering::Relaxed);
        }
        0x8 => {
            let _ = stream.write_all(&proto::encode_frame(0x8, &frame.payload)).await;
            return Err("closed".into());
        }
        0x9 => {
            stream
                .write_all(&proto::encode_frame(0xA, &frame.payload))
                .await
                .map_err(|err| err.to_string())?;
        }
        0xA => {
            // Pong received from server in response to client ping
        }
        _ => {}
    }
    Ok(())
}

async fn serve_metrics(port: u16, stats: Arc<Stats>) -> io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let body = stats.scrape();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let _ = timeout(Duration::from_secs(2), socket.read(&mut buf)).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

