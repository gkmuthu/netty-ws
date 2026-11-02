use crate::config::Config;

pub fn websocket_key() -> (String, String) {
    let mut raw = [0u8; 16];
    let mixed = next_u64().wrapping_mul(0x9E3779B97F4A7C15);
    let mixed_b = next_u64() ^ 0xA5A5_A5A5_A5A5_A5A5;
    raw[..8].copy_from_slice(&mixed.to_le_bytes());
    raw[8..].copy_from_slice(&mixed_b.to_le_bytes());
    let key = base64(&raw);
    let accept = accept_for(&key);
    (key, accept)
}

pub fn accept_for(key: &str) -> String {
    let mut material = Vec::with_capacity(key.len() + Config::accept_guid().len());
    material.extend_from_slice(key.as_bytes());
    material.extend_from_slice(Config::accept_guid().as_bytes());
    base64(&sha1(&material))
}

pub fn handshake_request(config: &Config, key: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: ws-load-client\r\n\r\n",
        path = config.path,
        host = config.host_header,
        key = key
    )
}

pub fn handshake_ok(head: &str, expected_accept: &str) -> Result<(), String> {
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap_or("");
    let code = status.split_whitespace().nth(1).unwrap_or("");
    if code != "101" {
        return Err(format!("handshake status {status}"));
    }
    let mut upgrade = false;
    let mut accept = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("upgrade") && value.eq_ignore_ascii_case("websocket") {
            upgrade = true;
        } else if name.eq_ignore_ascii_case("sec-websocket-accept") && value == expected_accept {
            accept = true;
        }
    }
    if !upgrade || !accept {
        return Err("handshake missing upgrade or accept".into());
    }
    Ok(())
}

pub fn encode_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mask = mask_key();
    let mut out = Vec::with_capacity(14 + payload.len());
    out.push(0x80 | (opcode & 0x0f));
    let len = payload.len();
    if len < 126 {
        out.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0x80 | 126);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0x80 | 127);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
    out.extend_from_slice(&mask);
    for (i, byte) in payload.iter().enumerate() {
        out.push(byte ^ mask[i & 3]);
    }
    out
}

#[derive(Debug)]
pub struct Frame {
    pub opcode: u8,
    pub fin: bool,
    pub payload: Vec<u8>,
}

pub fn pop_frame(buf: &mut Vec<u8>) -> Result<Option<Frame>, String> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let b0 = buf[0];
    let b1 = buf[1];
    if b0 & 0x70 != 0 {
        return Err("websocket extension bits are not accepted".into());
    }
    let masked = b1 & 0x80 != 0;
    let mut len = (b1 & 0x7f) as usize;
    let mut header = 2;
    if len == 126 {
        if buf.len() < 4 {
            return Ok(None);
        }
        len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        header = 4;
    } else if len == 127 {
        if buf.len() < 10 {
            return Ok(None);
        }
        let raw = u64::from_be_bytes(buf[2..10].try_into().unwrap());
        if raw > 65_536 {
            return Err("frame exceeds 65536 bytes".into());
        }
        len = raw as usize;
        header = 10;
    }
    if len > 65_536 {
        return Err("frame exceeds 65536 bytes".into());
    }
    let opcode = b0 & 0x0f;
    if opcode >= 0x8 && (len > 125 || b0 & 0x80 == 0) {
        return Err("invalid control frame".into());
    }
    if masked {
        header += 4;
    }
    if buf.len() < header + len {
        return Ok(None);
    }
    let mut payload = buf[header..header + len].to_vec();
    if masked {
        let mask = [
            buf[header - 4],
            buf[header - 3],
            buf[header - 2],
            buf[header - 1],
        ];
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[i & 3];
        }
    }
    let fin = b0 & 0x80 != 0;
    buf.drain(..header + len);
    Ok(Some(Frame { opcode, fin, payload }))
}

fn mask_key() -> [u8; 4] {
    let n = next_u64();
    let bytes = n.to_le_bytes();
    [bytes[0], bytes[1], bytes[2], bytes[3]]
}

fn next_u64() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);
    let n = SEQ.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let mut z = n.wrapping_add(0x6a09_e667_f3bc_c909);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn base64(data: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= data.len() {
        let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | data[i + 2] as u32;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
        out.push(TABLE[(n & 63) as usize] as char);
        i += 3;
    }
    if i < data.len() {
        let mut n = (data[i] as u32) << 16;
        if data.len() - i == 2 {
            n |= (data[i + 1] as u32) << 8;
        }
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        if data.len() - i == 2 {
            out.push(TABLE[((n >> 6) & 63) as usize] as char);
            out.push('=');
        } else {
            out.push('=');
            out.push('=');
        }
    }
    out
}

pub fn sha1(message: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x6745_2301;
    let mut h1: u32 = 0xEFCD_AB89;
    let mut h2: u32 = 0x98BA_DCFE;
    let mut h3: u32 = 0x1032_5476;
    let mut h4: u32 = 0xC3D2_E1F0;
    let bit_len = (message.len() as u64).saturating_mul(8);
    let mut data = Vec::with_capacity(message.len() + 72);
    data.extend_from_slice(message);
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in data.chunks_exact(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }
    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc6455_accept() {
        assert_eq!(accept_for("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn parses_unmasked_and_masked_text() {
        let mut plain = vec![0x81, 2, b'h', b'i'];
        let frame = pop_frame(&mut plain).unwrap().unwrap();
        assert_eq!(frame.payload, b"hi");
        assert!(plain.is_empty());

        let mut masked = encode_frame(0x1, b"hi");
        let frame = pop_frame(&mut masked).unwrap().unwrap();
        assert_eq!(frame.opcode, 0x1);
        assert_eq!(frame.payload, b"hi");
        assert!(masked.is_empty());
    }
}

