//! Gossip wire protocol: one request -> one response per exchange, self-
//! describing via a `type` tag so a single handler invocation (see
//! `bin/gossip_handler.rs`) can tell which of the two exchange kinds it's
//! being asked to do — required once a real CADS-Tunnel Agent-Fabric channel
//! call is a fresh, stateless subprocess per dial (ARCHITECTURE.md §7 step 2),
//! not just two lines read off a persistent stream (the original plain-TCP
//! transport this replaces still uses the same types, just still framed as
//! two JSON lines over one connection).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead, Read};
use vault_manifest::manifest::Manifest;

/// Bound on a single gossip message -- a request or response, whether framed as one line
/// over plain TCP (`main.rs`'s TCP transport) or as the whole of stdin for one
/// `gossip-handler` invocation (a fresh subprocess per incoming CADS-Tunnel channel call).
/// Both readers are otherwise unbounded (`read_line`/`read_to_string` grow until a
/// newline/EOF arrives): a malicious or merely misbehaving peer -- reachable over plain
/// TCP with no auth at all, or over an accepted channel call -- could send an
/// ever-growing stream and exhaust this process's memory with no upper bound. 256 MiB
/// comfortably covers a real vault's manifest snapshot (JSON, one entry per file) or a
/// full batch of missing-chunk responses (each chunk itself capped at
/// `vault_manifest::chunk::MAX_CHUNK_SIZE`, 64 KiB), while still being a real ceiling.
pub const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

fn too_large_err() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("gossip message exceeds MAX_MESSAGE_BYTES ({MAX_MESSAGE_BYTES})"),
    )
}

/// Read one line (the trailing `\n`, if any, is included) from `r`, erroring instead of
/// growing without bound if no newline arrives within [`MAX_MESSAGE_BYTES`]. `Ok(n == 0)`
/// (an immediately-closed connection) is left to the caller, matching `BufRead::read_line`.
pub fn read_bounded_line<R: BufRead>(r: &mut R) -> io::Result<String> {
    let mut line = String::new();
    let mut limited = r.take(MAX_MESSAGE_BYTES as u64 + 1);
    limited.read_line(&mut line)?;
    if line.len() > MAX_MESSAGE_BYTES {
        return Err(too_large_err());
    }
    Ok(line)
}

/// Read all of `r` into a `String`, erroring instead of growing without bound if the
/// source produces more than [`MAX_MESSAGE_BYTES`] before EOF.
pub fn read_bounded_to_string<R: Read>(r: &mut R) -> io::Result<String> {
    let mut buf = Vec::new();
    let mut limited = r.take(MAX_MESSAGE_BYTES as u64 + 1);
    limited.read_to_end(&mut buf)?;
    if buf.len() > MAX_MESSAGE_BYTES {
        return Err(too_large_err());
    }
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GossipRequest {
    Manifest { manifest: Manifest },
    GetChunks { hashes: Vec<String> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GossipResponse {
    Manifest { manifest: Manifest },
    /// hash (hex) -> base64-encoded chunk bytes. Only chunks the responder
    /// actually has are included; a requester that still has gaps after this
    /// will just try again next tick (possibly against a different peer).
    Chunks { chunks: HashMap<String, String> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn read_bounded_line_reads_a_normal_line_and_leaves_the_rest() {
        let mut r = Cursor::new(b"first line\nsecond line\n".to_vec());
        assert_eq!(read_bounded_line(&mut r).unwrap(), "first line\n");
        assert_eq!(read_bounded_line(&mut r).unwrap(), "second line\n");
    }

    #[test]
    fn read_bounded_line_refuses_an_over_limit_line_instead_of_growing_without_bound() {
        // The exact hazard this closes: a peer over plain TCP (no auth at all on that
        // transport) or an accepted channel call sends an ever-growing line with no
        // newline. `read_line` alone would keep buffering it into memory forever;
        // `read_bounded_line` must error once MAX_MESSAGE_BYTES is exceeded, not hang or
        // exhaust memory.
        let mut oversized = vec![b'x'; MAX_MESSAGE_BYTES + 10];
        oversized.push(b'\n');
        let mut r = Cursor::new(oversized);
        let err = read_bounded_line(&mut r).expect_err("an over-limit line must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("MAX_MESSAGE_BYTES"), "names the ceiling: {err}");
    }

    #[test]
    fn read_bounded_line_accepts_a_line_exactly_at_the_limit() {
        let mut at_limit = vec![b'x'; MAX_MESSAGE_BYTES - 1];
        at_limit.push(b'\n');
        let mut r = Cursor::new(at_limit.clone());
        let line = read_bounded_line(&mut r).expect("a line exactly at the limit is accepted");
        assert_eq!(line.len(), MAX_MESSAGE_BYTES);
    }

    #[test]
    fn read_bounded_to_string_reads_normally_and_refuses_over_limit() {
        let mut r = Cursor::new(b"hello world".to_vec());
        assert_eq!(read_bounded_to_string(&mut r).unwrap(), "hello world");

        let oversized = vec![b'y'; MAX_MESSAGE_BYTES + 1];
        let mut r = Cursor::new(oversized);
        let err = read_bounded_to_string(&mut r).expect_err("over-limit stdin must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("MAX_MESSAGE_BYTES"));
    }
}
