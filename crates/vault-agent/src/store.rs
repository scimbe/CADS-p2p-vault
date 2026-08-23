//! Local on-disk state for one agent: a content-addressed chunk store plus
//! the scan (local files -> manifest entries) and materialize (manifest
//! entries -> local files) directions. The manifest CRDT itself lives in
//! `vault_manifest`; this module is the only place that touches real bytes
//! on disk.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use vault_manifest::chunk;
use vault_manifest::hlc::Hlc;
use vault_manifest::manifest::{Entry, Manifest};

pub struct Store {
    pub root: PathBuf,
    pub vault_dir: PathBuf,
    pub chunks_dir: PathBuf,
}

impl Store {
    pub fn open(root: PathBuf) -> io::Result<Self> {
        let vault_dir = root.join(".vault");
        let chunks_dir = vault_dir.join("chunks");
        fs::create_dir_all(&root)?;
        fs::create_dir_all(&chunks_dir)?;
        Ok(Store { root, vault_dir, chunks_dir })
    }

    fn manifest_path(&self) -> PathBuf {
        self.vault_dir.join("manifest.json")
    }

    /// Load the on-disk manifest snapshot (empty if none exists yet). This is
    /// the only channel through which the long-lived agent process and the
    /// short-lived `gossip-handler` invocations (spawned fresh per incoming
    /// call by `ct-agent channel accept` — see bin/gossip_handler.rs) share
    /// state; they are separate OS processes, not threads.
    pub fn load_manifest(&self) -> io::Result<Manifest> {
        match fs::read(self.manifest_path()) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Manifest::new()),
            Err(e) => Err(e),
        }
    }

    /// Persist the manifest atomically (write-to-temp + rename) so a
    /// concurrently-spawned `gossip-handler` process never observes a
    /// partially-written file.
    pub fn save_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        let path = self.manifest_path();
        let tmp = self.vault_dir.join("manifest.json.tmp");
        let bytes = serde_json::to_vec(manifest).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, &path)
    }

    /// The on-disk path for chunk `hash_hex`, or `None` if `hash_hex` isn't a well-formed
    /// BLAKE3 hex hash (64 ASCII hex digits, what `chunk::chunk()` always produces). The
    /// single choke point every accessor below goes through: `hash_hex` reaches this crate
    /// both from our own chunking (always well-formed) AND, unvalidated, straight off the
    /// wire -- a `GossipResponse::Chunks` key a peer supplies (`main.rs::apply_chunks`) and
    /// an `Entry::chunk_hashes` value in a gossiped manifest. Without this check, a
    /// malicious/compromised peer could set `hash_hex` to something like
    /// `"../../../etc/cron.d/x"` and have `write_chunk` write attacker-controlled bytes to
    /// an arbitrary path outside the chunk store (or `read_chunk` read one back into the
    /// synced vault, if a matching file happens to exist there).
    fn chunk_path(&self, hash_hex: &str) -> Option<PathBuf> {
        let len_ok = hash_hex.len() == 64;
        let hex_ok = hash_hex.bytes().all(|b| b.is_ascii_hexdigit());
        (len_ok && hex_ok).then(|| self.chunks_dir.join(format!("{hash_hex}.bin")))
    }

    pub fn has_chunk(&self, hash_hex: &str) -> bool {
        self.chunk_path(hash_hex).is_some_and(|p| p.is_file())
    }

    pub fn write_chunk(&self, hash_hex: &str, bytes: &[u8]) -> io::Result<()> {
        let Some(path) = self.chunk_path(hash_hex) else {
            eprintln!("vault-agent: refusing to write chunk with malformed hash {hash_hex:?}");
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "malformed chunk hash"));
        };
        if path.is_file() {
            return Ok(()); // content-addressed: identical bytes already stored
        }
        fs::write(path, bytes)
    }

    pub fn read_chunk(&self, hash_hex: &str) -> io::Result<Vec<u8>> {
        let Some(path) = self.chunk_path(hash_hex) else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "malformed chunk hash"));
        };
        fs::read(path)
    }

    /// True if we already hold every chunk an entry needs.
    pub fn have_all_chunks(&self, entry: &Entry) -> bool {
        entry.chunk_hashes.iter().all(|h| self.has_chunk(h))
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// List every regular file under `root`, excluding the `.vault` bookkeeping
/// directory, as vault-relative forward-slash paths.
fn list_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.file_name().map(|n| n == ".vault").unwrap_or(false) {
                continue;
            }
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                out.push(path);
            }
        }
    }
    Ok(out)
}

fn rel_path(root: &Path, full: &Path) -> String {
    full.strip_prefix(root)
        .unwrap_or(full)
        .to_string_lossy()
        .replace('\\', "/")
}

/// `known_local[path]` is the entry we believe is *currently, correctly*
/// reflected on this agent's own disk for that path — set by `scan_local`
/// after authoring/confirming a file and by `materialize_remote` after
/// writing one. It is the single source of truth both directions share for
/// "did the user just delete this out from under us," which is what makes
/// deletion work for a file this agent didn't originally author (see below).
pub type KnownLocal = Mutex<HashMap<String, Entry>>;

/// Scan the local directory tree, chunk any new/changed files, and fold the
/// result into the local manifest under `self_id`'s authorship. Also tombs
/// any path this agent previously had materialized (regardless of who
/// originally authored it) whose file has since disappeared locally — every
/// agent is allowed to delete any file in the shared vault, not just files
/// it created itself.
pub fn scan_local(
    store: &Store,
    manifest: &Mutex<Manifest>,
    clock: &Mutex<Hlc>,
    known_local: &KnownLocal,
    self_id: &str,
) -> io::Result<Vec<String>> {
    let mut changed = Vec::new();
    let files = list_files(&store.root)?;
    let mut seen: HashSet<String> = HashSet::new();

    for full_path in &files {
        let rel = rel_path(&store.root, full_path);
        seen.insert(rel.clone());

        let bytes = fs::read(full_path)?;
        let chunks = chunk::chunk(&bytes);
        for c in &chunks {
            let hash_hex = c.hash.to_hex().to_string();
            let slice = &bytes[c.offset..c.offset + c.len];
            store.write_chunk(&hash_hex, slice)?;
        }
        let chunk_hashes: Vec<String> = chunks.iter().map(|c| c.hash.to_hex().to_string()).collect();
        let size = bytes.len() as u64;

        // Whether this is a genuine LOCAL edit must be judged against what
        // *this agent* last confirmed was correctly on its own disk
        // (`known_local`), never against the manifest's current global
        // entry. The manifest can be ahead of this replica's disk simply
        // because a peer's edit hasn't been gossip-fetched and materialized
        // yet — that is `materialize_remote`'s job, not a local edit. A
        // real 3-agent run caught two related bugs from getting this wrong
        // the first two times:
        //   1. Comparing to the manifest entry meant a materialized-but-
        //      unmodified file (content already matches what a peer wrote)
        //      still looked "changed" from the receiving agent's own point
        //      of view, manufacturing a spurious conflict purely from local
        //      disk churn.
        //   2. Comparing to the manifest entry ALSO meant: the moment any
        //      agent learned of a newer edit or delete from a peer (via
        //      gossip) but hadn't yet materialized it to disk, its own next
        //      scan saw "my disk disagrees with the manifest" and
        //      re-authored ITS OWN stale content under a fresh (and thus
        //      always-winning) HLC — silently reverting the real edit, or
        //      un-deleting a tombstoned file, for every replica that wasn't
        //      the one making the change.
        // Comparing against `known_local` instead means: no discrepancy, no
        // action — leave it for `materialize_remote` to catch up.
        let known_entry = known_local.lock().unwrap().get(&rel).cloned();
        let matches_known = matches!(&known_entry,
            Some(e) if !e.tombstone && e.chunk_hashes == chunk_hashes && e.size == size);
        if matches_known {
            continue;
        }

        let hlc = clock.lock().unwrap().tick(now_ms());
        let entry = Entry {
            chunk_hashes,
            size,
            hlc,
            author_pubkey: self_id.to_string(),
            tombstone: false,
        };
        manifest.lock().unwrap().put(&rel, entry.clone());
        known_local.lock().unwrap().insert(rel.clone(), entry);
        changed.push(rel);
    }

    // Tomb any path we'd previously confirmed on disk (ours or a peer's)
    // that the local filesystem no longer has.
    let previously_known: Vec<String> = known_local.lock().unwrap().keys().cloned().collect();
    for path in previously_known {
        let still_live = known_local
            .lock()
            .unwrap()
            .get(&path)
            .map(|e| !e.tombstone)
            .unwrap_or(false);
        if still_live && !seen.contains(&path) {
            let hlc = clock.lock().unwrap().tick(now_ms());
            let entry = Entry {
                chunk_hashes: vec![],
                size: 0,
                hlc,
                author_pubkey: self_id.to_string(),
                tombstone: true,
            };
            manifest.lock().unwrap().put(&path, entry.clone());
            known_local.lock().unwrap().insert(path.clone(), entry);
            changed.push(path);
        }
    }

    Ok(changed)
}

/// Apply manifest entries authored by *other* agents to local disk: write
/// files we now have every chunk for, delete files behind a tombstone.
/// Entries we can't fully materialize yet (still missing chunk bytes) are
/// left for a later tick, after gossip has had a chance to fetch them.
pub fn materialize_remote(
    store: &Store,
    manifest: &Mutex<Manifest>,
    self_id: &str,
    known_local: &KnownLocal,
) -> io::Result<Vec<String>> {
    let mut applied = Vec::new();
    let snapshot: Vec<(String, Entry)> = {
        let m = manifest.lock().unwrap();
        m.iter().map(|(p, e)| (p.clone(), e.clone())).collect()
    };

    for (path, entry) in snapshot {
        if entry.author_pubkey == self_id {
            continue;
        }
        {
            let kl = known_local.lock().unwrap();
            if kl.get(&path) == Some(&entry) {
                continue;
            }
        }

        let full_path = store.root.join(&path);
        if entry.tombstone {
            match fs::remove_file(&full_path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            known_local.lock().unwrap().insert(path.clone(), entry);
            applied.push(path);
            continue;
        }

        if !store.have_all_chunks(&entry) {
            continue; // wait for gossip to backfill the missing chunk bytes
        }
        let mut bytes = Vec::with_capacity(entry.size as usize);
        for hash in &entry.chunk_hashes {
            bytes.extend_from_slice(&store.read_chunk(hash)?);
        }
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&full_path, &bytes)?;
        known_local.lock().unwrap().insert(path.clone(), entry);
        applied.push(full_path.to_string_lossy().to_string());
    }

    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> Store {
        let dir = std::env::temp_dir().join(format!(
            "vault-store-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        Store::open(dir).expect("open store")
    }

    #[test]
    fn write_chunk_refuses_a_traversal_hash_instead_of_escaping_the_chunks_dir() {
        // The exact attack this closes: `apply_chunks` (main.rs) writes chunk bytes
        // using a hash straight off the wire -- a key in a peer's `GossipResponse::
        // Chunks` map, entirely attacker-controlled. Without a shape check, a
        // malicious/compromised peer could set that key to a traversal path and get
        // arbitrary bytes written outside the chunk store (and outside the vault
        // entirely). A well-formed hash is exactly 64 ASCII hex digits (what
        // `chunk::chunk()`'s BLAKE3 `to_hex()` always produces) -- anything else is
        // refused before any filesystem call.
        let store = temp_store();
        let err = store
            .write_chunk("../../../etc/cron.d/evil", b"malicious content")
            .expect_err("a traversal hash must be refused, not written");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            !store.root.parent().unwrap().join("etc").join("cron.d").join("evil").exists(),
            "nothing was written outside the store"
        );

        // Same for a too-short/too-long or non-hex value -- any shape that isn't a
        // real 64-hex-digit hash is refused, not silently accepted.
        assert!(store.write_chunk("short", b"x").is_err());
        assert!(store.write_chunk(&"a".repeat(63), b"x").is_err(), "63 chars, one short");
        assert!(store.write_chunk(&"a".repeat(65), b"x").is_err(), "65 chars, one long");
        assert!(store.write_chunk(&"g".repeat(64), b"x").is_err(), "non-hex character");

        // A genuine 64-hex-digit hash still works normally.
        let real_hash = "a".repeat(64);
        store.write_chunk(&real_hash, b"real bytes").expect("a well-formed hash is accepted");
        assert_eq!(store.read_chunk(&real_hash).unwrap(), b"real bytes");

        let _ = fs::remove_dir_all(&store.root);
    }

    #[test]
    fn has_chunk_and_read_chunk_also_refuse_a_malformed_hash() {
        let store = temp_store();
        assert!(!store.has_chunk("../../../etc/passwd"), "malformed hash is never reported as present");
        assert_eq!(
            store.read_chunk("../../../etc/passwd").unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
            "malformed hash is refused, not passed through to fs::read"
        );
        let _ = fs::remove_dir_all(&store.root);
    }
}
