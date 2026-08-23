//! The manifest CRDT: an OR-Set of `path -> entry`, LWW by HLC per path, with
//! conflict-copy retention when two writes to the same path are truly concurrent
//! (unordered — same author-less tie or a clock race). See ARCHITECTURE.md §2.
//!
//! A file's content is represented as an ordered list of content-defined-chunk
//! hashes (`crate::chunk`), not one whole-file hash: editing part of a large
//! file only changes the chunks around the edit, so peers only need to
//! transfer/store those chunks, not the whole file again (see ARCHITECTURE.md
//! §2's chunking rationale — the earlier whole-blob design didn't have this
//! property and was replaced).

use crate::hlc::Hlc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Ordered content-defined-chunk hashes (hex-encoded BLAKE3) making up this
    /// file's bytes, in order. Two files/versions that share long runs of
    /// identical content share chunk hashes even at different offsets.
    pub chunk_hashes: Vec<String>,
    pub size: u64,
    pub hlc: Hlc,
    pub author_pubkey: String,
    pub tombstone: bool,
}

impl Entry {
    /// Deterministic content identity for this entry, used only for
    /// conflict-copy winner tie-breaking (see `apply` below) — NOT a content
    /// hash of the whole file, just a stable ordering key over the chunk list.
    fn content_key(&self) -> String {
        self.chunk_hashes.join(",")
    }
}

/// Whether `path` is safe to later join onto a filesystem root without escaping it: not
/// empty, not absolute, and no `..` (parent-directory) component anywhere in it. Pure and
/// deliberately conservative -- a `.` (current-dir) component is also rejected, since no
/// legitimate path this crate ever produces (`vault-agent`'s `rel_path`, a plain
/// `strip_prefix` of a real directory walk) contains one.
fn is_safe_relative_path(path: &str) -> bool {
    use std::path::{Component, Path};
    if path.is_empty() {
        return false;
    }
    Path::new(path).components().all(|c| matches!(c, Component::Normal(_)))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    entries: HashMap<String, Entry>,
}

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, path: &str, entry: Entry) {
        self.apply(path.to_string(), entry);
    }

    pub fn get(&self, path: &str) -> Option<&Entry> {
        self.entries.get(path).filter(|e| !e.tombstone)
    }

    /// Like `get`, but also returns tombstoned entries. Callers that decide
    /// whether to (re-)author a path from local disk state need this: a file
    /// physically present locally but tombstoned by another agent must NOT
    /// be resurrected just because `get` hides the tombstone from them.
    pub fn get_any(&self, path: &str) -> Option<&Entry> {
        self.entries.get(path)
    }

    pub fn len(&self) -> usize {
        self.entries.values().filter(|e| !e.tombstone).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate all entries (including tombstones) — callers that materialize
    /// files onto disk need to see tombstones too, unlike `get`.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Entry)> {
        self.entries.iter()
    }

    /// Merge a remote manifest into this one. Deterministic: every replica that
    /// observes the same set of entries converges to the same result regardless
    /// of merge order (associative + commutative + idempotent, as required of a CRDT).
    pub fn merge(&mut self, other: &Manifest) {
        for (path, entry) in &other.entries {
            self.apply(path.clone(), entry.clone());
        }
    }

    fn apply(&mut self, path: String, incoming: Entry) {
        if !is_safe_relative_path(&path) {
            // Manifest entries arrive over gossip from network peers (see `merge`) with
            // `path` as an attacker-controlled string key. A consumer that materializes
            // entries onto disk (`vault-agent`'s `materialize_remote`) joins `path` onto
            // its vault root; without this check here — the single choke point every
            // `put`/`merge` goes through — a malicious/compromised peer could set
            // `path = "../../../etc/cron.d/x"` and have that consumer write (or, via a
            // tombstone, delete) an arbitrary file outside the vault directory entirely.
            eprintln!(
                "vault-manifest: refusing entry with unsafe path {path:?} (must be a \
                 relative path with no '..' component) -- a malicious peer cannot use \
                 this to escape the vault directory"
            );
            return;
        }
        match self.entries.get(&path) {
            None => {
                self.entries.insert(path, incoming);
            }
            Some(existing) => {
                if incoming.hlc > existing.hlc {
                    self.entries.insert(path, incoming);
                } else if incoming.hlc == existing.hlc && incoming != *existing {
                    // Truly concurrent (identical HLC, different content) — never
                    // silently drop either side. Deterministic tie-break by
                    // content_key keeps the "primary" path stable across
                    // replicas; the loser survives under a conflict-suffixed path.
                    let (winner, loser) = if incoming.content_key() >= existing.content_key() {
                        (incoming.clone(), existing.clone())
                    } else {
                        (existing.clone(), incoming.clone())
                    };
                    let conflict_path = format!(
                        "{path}.conflict-{}",
                        &loser.author_pubkey[..loser.author_pubkey.len().min(8)]
                    );
                    self.entries.insert(path, winner);
                    self.entries.entry(conflict_path).or_insert(loser);
                }
                // incoming.hlc < existing.hlc: existing already wins, nothing to do.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(hash: &str, hlc: Hlc, author: &str) -> Entry {
        Entry {
            chunk_hashes: vec![hash.to_string()],
            size: 1,
            hlc,
            author_pubkey: author.to_string(),
            tombstone: false,
        }
    }

    #[test]
    fn is_safe_relative_path_rejects_traversal_and_absolute_paths() {
        assert!(is_safe_relative_path("a.txt"));
        assert!(is_safe_relative_path("dir/a.txt"));
        assert!(is_safe_relative_path("dir/sub/a.txt"));
        assert!(!is_safe_relative_path(""), "empty path");
        assert!(!is_safe_relative_path("../etc/passwd"), "leading traversal");
        assert!(!is_safe_relative_path("dir/../../etc/passwd"), "embedded traversal");
        assert!(!is_safe_relative_path("a/../../b"), "traversal past the root");
        assert!(!is_safe_relative_path("/etc/passwd"), "absolute path");
        assert!(!is_safe_relative_path("./a.txt"), "current-dir component");
    }

    #[test]
    fn a_gossiped_entry_with_a_traversal_path_is_refused_not_merged() {
        // The exact attack this closes: a malicious/compromised gossip peer sets an
        // entry's path to escape the vault root once a consumer (vault-agent's
        // materialize_remote) joins it onto the local vault directory. The manifest
        // must refuse to even hold such an entry -- proven here against the ONE
        // choke point (`apply`, via both `put` and `merge`) every entry goes through.
        let mut m = Manifest::new();
        let evil = entry("h1", Hlc { physical_ms: 1, logical: 0 }, "mallory");
        m.put("../../../etc/cron.d/evil", evil.clone());
        assert!(m.is_empty(), "a traversal path must never be stored");
        assert_eq!(m.get_any("../../../etc/cron.d/evil"), None);

        // Same via merge (the actual gossip-ingest path).
        let mut peer = Manifest::new();
        peer.put("legit.txt", entry("h2", Hlc { physical_ms: 1, logical: 0 }, "mallory"));
        // Bypass `put`'s own guard to construct an attacker manifest as it would
        // arrive over the wire (a real peer controls its own serialized JSON, not
        // our `put` API) -- merge must still refuse it on OUR side.
        peer.entries.insert(
            "../outside.txt".to_string(),
            entry("h3", Hlc { physical_ms: 1, logical: 0 }, "mallory"),
        );
        let mut local = Manifest::new();
        local.merge(&peer);
        assert_eq!(local.len(), 1, "only the safe entry merges");
        assert!(local.get("legit.txt").is_some());
        assert_eq!(local.get_any("../outside.txt"), None, "the traversal entry never merges in");
    }

    #[test]
    fn get_hides_tombstones_but_get_any_still_sees_them() {
        // Regression test for a real bug found running vault-agent locally:
        // an agent that still physically has a file another agent deleted
        // used `get()` (which hides tombstones) to decide whether to
        // re-author it, so it always looked "missing" and got resurrected
        // with a fresh HLC that clobbered the delete.
        let mut m = Manifest::new();
        let mut e = entry("h1", Hlc { physical_ms: 1, logical: 0 }, "alice");
        m.put("a.txt", e.clone());
        e.tombstone = true;
        e.hlc = Hlc { physical_ms: 2, logical: 0 };
        m.put("a.txt", e);

        assert!(m.get("a.txt").is_none(), "get() must hide a tombstoned entry");
        assert!(
            m.get_any("a.txt").is_some_and(|e| e.tombstone),
            "get_any() must still expose it so a stale local file isn't resurrected"
        );
    }

    #[test]
    fn later_hlc_wins_on_the_same_path() {
        let mut m = Manifest::new();
        m.put("a.txt", entry("h1", Hlc { physical_ms: 1, logical: 0 }, "alice"));
        m.put("a.txt", entry("h2", Hlc { physical_ms: 2, logical: 0 }, "bob"));
        assert_eq!(m.get("a.txt").unwrap().chunk_hashes, vec!["h2"]);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn true_concurrency_keeps_both_as_a_conflict_copy() {
        let mut m = Manifest::new();
        let same_hlc = Hlc { physical_ms: 5, logical: 0 };
        m.put("a.txt", entry("hAAA", same_hlc, "alice"));
        m.put("a.txt", entry("hBBB", same_hlc, "bob"));
        assert_eq!(m.len(), 2, "both writers' data must survive a true conflict");
    }

    #[test]
    fn merge_converges_regardless_of_order() {
        let mut m1 = Manifest::new();
        m1.put("a.txt", entry("h1", Hlc { physical_ms: 1, logical: 0 }, "alice"));
        m1.put("b.txt", entry("h2", Hlc { physical_ms: 1, logical: 0 }, "alice"));

        let mut m2 = Manifest::new();
        m2.put("a.txt", entry("h3", Hlc { physical_ms: 2, logical: 0 }, "bob"));
        m2.put("c.txt", entry("h4", Hlc { physical_ms: 1, logical: 0 }, "bob"));

        let mut left_then_right = m1.clone();
        left_then_right.merge(&m2);

        let mut right_then_left = m2.clone();
        right_then_left.merge(&m1);

        assert_eq!(left_then_right.get("a.txt").unwrap().chunk_hashes, vec!["h3"]);
        assert_eq!(right_then_left.get("a.txt").unwrap().chunk_hashes, vec!["h3"]);
        assert_eq!(left_then_right.len(), right_then_left.len());
        assert_eq!(left_then_right.len(), 3); // a.txt (h3), b.txt, c.txt
    }
}
