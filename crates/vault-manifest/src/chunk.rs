//! Content-defined chunking (buzhash: a true bounded sliding-window rolling hash).
//!
//! Fixed-size chunking suffers the "boundary-shift" problem: inserting one byte
//! near the start of a large file shifts every subsequent fixed-size block,
//! destroying dedup/delta-sync for the whole rest of the file. Content-defined
//! chunking places boundaries based on a rolling hash of the *content*, so an
//! edit only invalidates the chunks around it — everything before and after
//! re-uses the same chunk hashes it already had. This is why restic, Borg,
//! Perkeep, and FastCDC-based systems all chunk this way instead of by fixed
//! offset. See ARCHITECTURE.md §2 for the citations behind this choice.
//!
//! **Correctness note (found by the test suite below, not assumed):** an
//! earlier version of this file used an add-based "Gear hash"
//! (`hash = (hash << 1) + table[byte]`, reset or not reset at each cut) and its
//! resync tests failed either way. An add-based accumulator doesn't cleanly
//! forget old bytes — carries from `wrapping_add` propagate upward with no
//! fixed lifetime, so a single early difference can perturb the hash for far
//! longer than the nominal "window," breaking the whole property CDC exists
//! for. **Buzhash** (`rotate_left` + XOR) is used instead: XOR is its own
//! inverse, so XOR-ing out `rotate_left(table[byte_leaving], WINDOW)` exactly
//! cancels that byte's contribution once it's `WINDOW` bytes in the past — a
//! genuinely bounded window, not an approximate one. This is what actually
//! gives the resynchronize-after-an-edit and cross-file-dedup properties
//! (verified below), which is the entire reason to chunk this way over fixed
//! offsets.
//!
//! Single-mask (not FastCDC's dual-mask size-normalization) — simpler, still
//! has the core content-defined-boundary property; dual-mask normalization
//! (tighter chunk-size distribution) is a possible follow-up, not a
//! correctness requirement.

use blake3::Hash;

pub const MIN_CHUNK_SIZE: usize = 4 * 1024; // 4 KiB
pub const AVG_CHUNK_SIZE: usize = 16 * 1024; // 16 KiB — the mask is derived from this
pub const MAX_CHUNK_SIZE: usize = 64 * 1024; // 64 KiB hard cap

/// Bytes of true rolling-hash history retained before a byte's contribution is
/// XOR-cancelled out. Must be < 64 (bits in the rotated word).
const WINDOW: usize = 48;

// mask ≈ AVG_CHUNK_SIZE - 1 rounded down to nearest power-of-two minus one, so a
// boundary is expected roughly every AVG_CHUNK_SIZE bytes (2^14 = 16384).
const CUT_MASK: u64 = (1u64 << 14) - 1;

/// A fixed, precomputed table of pseudo-random 64-bit values indexed by byte
/// value — the buzhash lookup table. Deterministic (same table on every
/// agent) so the same bytes always produce the same chunk boundaries anywhere.
fn buzhash_table() -> &'static [u64; 256] {
    static TABLE: std::sync::OnceLock<[u64; 256]> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [0u64; 256];
        // Deterministically derived from BLAKE3 rather than an RNG, so the table
        // is reproducible from source with no embedded magic constants to trust.
        for (i, slot) in table.iter_mut().enumerate() {
            let h = blake3::hash(&[i as u8]);
            let bytes: [u8; 8] = h.as_bytes()[0..8].try_into().unwrap();
            *slot = u64::from_le_bytes(bytes);
        }
        table
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub hash: Hash,
    pub offset: usize,
    pub len: usize,
}

/// Split `data` into content-defined chunks. Deterministic: identical bytes
/// anywhere in `data` (or in a different file entirely) produce an identical
/// chunk hash, which is what makes cross-file/cross-version deduplication and
/// delta-sync (only transfer chunks the peer doesn't already have) both work.
pub fn chunk(data: &[u8]) -> Vec<Chunk> {
    if data.is_empty() {
        return Vec::new();
    }
    let table = buzhash_table();
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut hash: u64 = 0;

    for i in 0..data.len() {
        hash = hash.rotate_left(1) ^ table[data[i] as usize];
        // Cancel out the byte that just fell off the back of the window, based
        // on the ABSOLUTE position `i`, never on `start` — the whole point is
        // that `hash` must depend only on the trailing WINDOW bytes of *file*
        // content, never on where the previous cut happened to land. (An
        // earlier version reset `hash = 0` at each cut and gated cancellation
        // on `i >= start + WINDOW`; that reintroduces a dependency on cut
        // history and silently broke resync — caught by the tests below, not
        // assumed away.)
        if i >= WINDOW {
            let leaving = data[i - WINDOW];
            hash ^= table[leaving as usize].rotate_left((WINDOW % 64) as u32);
        }

        let len = i - start + 1;
        let window_full = i >= WINDOW;
        let at_boundary = window_full && (hash & CUT_MASK) == 0 && len >= MIN_CHUNK_SIZE;
        let forced = len >= MAX_CHUNK_SIZE;
        let is_last_byte = i == data.len() - 1;
        if at_boundary || forced || is_last_byte {
            let slice = &data[start..=i];
            chunks.push(Chunk { hash: blake3::hash(slice), offset: start, len: slice.len() });
            start = i + 1;
        }
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_produces_no_chunks() {
        assert!(chunk(&[]).is_empty());
    }

    #[test]
    fn chunking_is_deterministic() {
        let data = vec![b'x'; 200_000];
        assert_eq!(chunk(&data), chunk(&data));
    }

    #[test]
    fn no_chunk_exceeds_the_max_size() {
        // Highly compressible/repetitive input is exactly the adversarial case
        // for boundary-finding — verify the hard cap actually holds.
        let data = vec![7u8; 500_000];
        for c in chunk(&data) {
            assert!(c.len <= MAX_CHUNK_SIZE, "chunk of {} bytes exceeds cap", c.len);
        }
    }

    // A tiny deterministic PRNG (splitmix64) for test data — real files are
    // not periodic or uniform, and testing CDC against degenerate input
    // (all-same-byte, or a short repeating ramp) mostly exercises the
    // MAX_CHUNK_SIZE forced-cut fallback rather than genuine content-defined
    // boundaries, which would make these tests pass or fail for the wrong
    // reason either way.
    fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            state = state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
            out.extend_from_slice(&z.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    #[test]
    fn inserting_bytes_near_the_start_only_perturbs_nearby_chunks() {
        // The whole point of content-defined chunking over fixed-size blocks:
        // a small edit near the front must NOT invalidate every chunk hash for
        // the rest of the file.
        let original = pseudo_random_bytes(300_000, 0xC0FFEE);
        let mut edited = original.clone();
        edited.splice(10..10, [0xFFu8; 3]); // insert 3 bytes near the start

        let original_chunks = chunk(&original);
        let edited_chunks = chunk(&edited);

        let original_hashes: std::collections::HashSet<_> =
            original_chunks.iter().map(|c| c.hash).collect();
        let edited_hashes: std::collections::HashSet<_> =
            edited_chunks.iter().map(|c| c.hash).collect();
        let shared = original_hashes.intersection(&edited_hashes).count();

        // With fixed-size chunking, a 3-byte insertion near the start would
        // shift every following block and share ~0 chunks with the original.
        // Content-defined chunking should re-converge quickly after the edit
        // and share the large majority of chunks from there on.
        let shared_fraction = shared as f64 / original_hashes.len() as f64;
        assert!(
            shared_fraction > 0.7,
            "expected most chunks to survive a small edit, only {:.0}% did",
            shared_fraction * 100.0
        );
    }

    #[test]
    fn identical_content_in_different_files_dedups_to_the_same_chunk_hash() {
        let shared_block = pseudo_random_bytes(50_000, 0xFEED_BEEF);
        let mut file_a = shared_block.clone();
        file_a.extend_from_slice(b"file-a-specific-tail");
        let mut file_b = b"file-b-specific-head".to_vec();
        file_b.extend_from_slice(&shared_block);

        let a_hashes: std::collections::HashSet<_> =
            chunk(&file_a).into_iter().map(|c| c.hash).collect();
        let b_hashes: std::collections::HashSet<_> =
            chunk(&file_b).into_iter().map(|c| c.hash).collect();
        assert!(
            a_hashes.intersection(&b_hashes).count() > 0,
            "the shared 50KB block should dedup across two otherwise-different files"
        );
    }
}
