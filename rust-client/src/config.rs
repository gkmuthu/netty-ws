use std::env;
use std::net::{IpAddr, SocketAddr};

const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub host_header: String,
    pub connections: u32,
    pub ramp_per_second: u32,
    pub initial_delay_seconds: u64,
    pub hold_seconds: u64,
    pub ping_seconds: u64,
    pub message_bytes: usize,
    pub message_text: String,
    pub message_interval_seconds: u64,
    pub workers: usize,
    pub metrics_port: u16,
    pub report_seconds: u64,
    pub recv_buffer: usize,
    pub send_buffer: usize,
    pub local_addresses: Vec<SocketAddr>,
}

impl Config {
    pub fn from_args() -> Result<Self, String> {
        let mut values = env::vars().collect::<std::collections::HashMap<_, _>>();
        apply_args(&mut values, env::args().skip(1));
        Self::from_map(&values)
    }

    fn from_map(values: &std::collections::HashMap<String, String>) -> Result<Self, String> {
        let target = text(values, "TARGET", "");
        if target.is_empty() {
            return Err("TARGET is required, for example ws://127.0.0.1:8080/ws".into());
        }
        let (host, port, path, host_header) = parse_target(&target)?;
        let connections = number(values, "CONNECTIONS", 1)?;
        let ramp_per_second = number(values, "RAMP_PER_SECOND", 200)?;
        let initial_delay_seconds = number(values, "INITIAL_DELAY", 0)?;
        let hold_seconds = number(values, "HOLD_SECONDS", 0)?;
        let ping_seconds = number(values, "PING_SECONDS", 30)?;
        let message_bytes = number(values, "MESSAGE_BYTES", 0)?;
        let message_text = text(values, "MESSAGE_TEXT", "");
        let message_interval_seconds = number(values, "MESSAGE_INTERVAL_SECONDS", 10)?;
        let workers = number(values, "WS_WORKERS", default_workers() as u64)?;
        let metrics_port = number(values, "METRICS_PORT", 0)?;
        let report_seconds = number(values, "REPORT_SECONDS", 5)?;
        let recv_buffer = number(values, "WS_SO_RCVBUF", 4096)?;
        let send_buffer = number(values, "WS_SO_SNDBUF", 4096)?;
        if connections < 1
            || ramp_per_second < 1
            || message_interval_seconds < 1
            || workers < 1
            || metrics_port > 65535
            || report_seconds < 1
        {
            return Err("invalid client configuration".into());
        }
        let local_addresses = text(values, "LOCAL_ADDRESSES", "")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<IpAddr>()
                    .map(|ip| SocketAddr::new(ip, 0))
                    .map_err(|_| format!("invalid LOCAL_ADDRESSES entry: {s}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            host,
            port,
            path,
            host_header,
            connections: connections as u32,
            ramp_per_second: ramp_per_second as u32,
            initial_delay_seconds,
            hold_seconds,
            ping_seconds,
            message_bytes: message_bytes as usize,
            message_text,
            message_interval_seconds,
            workers: workers as usize,
            metrics_port: metrics_port as u16,
            report_seconds,
            recv_buffer: recv_buffer as usize,
            send_buffer: send_buffer as usize,
            local_addresses,
        })
    }

    pub fn summary(&self) -> String {
        format!(
            "target=ws://{}:{}{} connections={} rampPerSecond={} initialDelaySeconds={} holdSeconds={} localAddresses={} messageBytes={} messageText={} metricsPort={}",
            self.host,
            self.port,
            self.path,
            self.connections,
            self.ramp_per_second,
            self.initial_delay_seconds,
            if self.hold_seconds == 0 {
                "forever".to_string()
            } else {
                self.hold_seconds.to_string()
            },
            self.local_addresses.len(),
            self.message_bytes,
            !self.message_text.is_empty(),
            self.metrics_port
        )
    }

    pub fn payload(&self) -> Option<Vec<u8>> {
        if !self.message_text.is_empty() {
            Some(self.message_text.as_bytes().to_vec())
        } else if self.message_bytes > 0 {
            Some(vec![0; self.message_bytes])
        } else {
            None
        }
    }

    pub fn accept_guid() -> &'static str {
        GUID
    }
}

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(1)
}

fn text(values: &std::collections::HashMap<String, String>, name: &str, fallback: &str) -> String {
    values
        .get(name)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn number(values: &std::collections::HashMap<String, String>, name: &str, fallback: u64) -> Result<u64, String> {
    match values.get(name).map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(fallback),
        Some(value) => value
            .parse::<u64>()
            .map_err(|_| format!("{name} must be an integer")),
    }
}

fn apply_args(values: &mut std::collections::HashMap<String, String>, args: impl Iterator<Item = String>) {
    let args: Vec<String> = args.collect();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if !arg.starts_with("--") {
            i += 1;
            continue;
        }
        let body = &arg[2..];
        let (key, value) = if let Some((key, value)) = body.split_once('=') {
            (key.to_string(), value.to_string())
        } else if i + 1 < args.len() && !args[i + 1].starts_with("--") {
            i += 1;
            (body.to_string(), args[i].clone())
        } else {
            (body.to_string(), "true".to_string())
        };
        values.insert(arg_key(&key), value);
        i += 1;
    }
}

fn arg_key(key: &str) -> String {
    match key {
        "target" => "TARGET",
        "connections" => "CONNECTIONS",
        "ramp-per-second" => "RAMP_PER_SECOND",
        "initial-delay" | "initial-delay-seconds" => "INITIAL_DELAY",
        "hold-seconds" => "HOLD_SECONDS",
        "ping-seconds" => "PING_SECONDS",
        "message-bytes" => "MESSAGE_BYTES",
        "message-text" => "MESSAGE_TEXT",
        "message-interval-seconds" => "MESSAGE_INTERVAL_SECONDS",
        "workers" => "WS_WORKERS",
        "local-addresses" => "LOCAL_ADDRESSES",
        "metrics-port" => "METRICS_PORT",
        "report-seconds" => "REPORT_SECONDS",
        "so-rcvbuf" => "WS_SO_RCVBUF",
        "so-sndbuf" => "WS_SO_SNDBUF",
        "mode" => "MODE",
        other => other,
    }
    .to_string()
}

fn parse_target(target: &str) -> Result<(String, u16, String, String), String> {
    let rest = target
        .strip_prefix("ws://")
        .ok_or_else(|| "TARGET must be ws://host:port/path (TLS is not used by this client)".to_string())?;
    if rest.contains('@') {
        return Err("TARGET userinfo is not supported".into());
    }
    let (host, after_host) = if let Some(rest) = rest.strip_prefix('[') {
        let end = rest.find(']').ok_or("invalid IPv6 TARGET")?;
        (rest[..end].to_string(), &rest[end + 1..])
    } else {
        let end = rest.find([':', '/']).unwrap_or(rest.len());
        (rest[..end].to_string(), &rest[end..])
    };
    if host.is_empty() {
        return Err("TARGET host is empty".into());
    }
    let (port, path) = if let Some(after_port) = after_host.strip_prefix(':') {
        let end = after_port.find('/').unwrap_or(after_port.len());
        let port = after_port[..end]
            .parse::<u16>()
            .map_err(|_| "TARGET port must be an integer".to_string())?;
        let path = if end == after_port.len() { "/" } else { &after_port[end..] };
        (port, path.to_string())
    } else if after_host.is_empty() {
        (80, "/".to_string())
    } else if let Some(path) = after_host.strip_prefix('/') {
        (80, format!("/{path}"))
    } else {
        return Err("invalid TARGET".into());
    };
    if !path.starts_with('/') {
        return Err("TARGET path must start with /".into());
    }
    let host_header = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    Ok((host, port, path, host_header))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn parses_target_and_args() {
        let (host, port, path, header) = parse_target("ws://ws:8080/ws").unwrap();
        assert_eq!(host, "ws");
        assert_eq!(port, 8080);
        assert_eq!(path, "/ws");
        assert_eq!(header, "ws:8080");
        let mut values = HashMap::from([("CONNECTIONS".into(), "1".into())]);
        apply_args(
            &mut values,
            ["--target=ws://127.0.0.1:8080/ws", "--connections", "10", "--initial-delay=5", "--hold-seconds=0", "--mode=client"]
                .into_iter()
                .map(str::to_string),
        );
        let config = Config::from_map(&values).unwrap();
        assert_eq!(config.connections, 10);
        assert_eq!(config.port, 8080);
        assert_eq!(config.path, "/ws");
        assert_eq!(config.initial_delay_seconds, 5);
        assert_eq!(config.hold_seconds, 0);
        assert_eq!(config.ping_seconds, 30);
    }
}

