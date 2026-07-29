//! Provider-lease CRDT: leaderless failover without a consensus round.
//! See ARCHITECTURE.md §4.

use crate::hlc::Hlc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ProviderLease<'a> {
    pub epoch: u64,
    pub holder_pubkey: &'a str,
    pub connectivity_score: f32,
    pub renewed_at: Hlc,
}

/// Merge two observed leases deterministically: higher epoch always wins (it's a
/// later takeover); within the same epoch, higher connectivity_score wins; ties
/// broken by holder_pubkey so every replica picks the same winner independently.
pub fn merge_lease<'a>(a: ProviderLease<'a>, b: ProviderLease<'a>) -> ProviderLease<'a> {
    if a.epoch != b.epoch {
        return if a.epoch > b.epoch { a } else { b };
    }
    if a.connectivity_score != b.connectivity_score {
        return if a.connectivity_score > b.connectivity_score { a } else { b };
    }
    if a.holder_pubkey <= b.holder_pubkey {
        a
    } else {
        b
    }
}

pub fn connectivity_score(direct_peer_count: u32, rtt_to_edge_ms: f32) -> f32 {
    (direct_peer_count as f32) * 10.0 - rtt_to_edge_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease<'a>(epoch: u64, holder: &'a str, score: f32) -> ProviderLease<'a> {
        ProviderLease {
            epoch,
            holder_pubkey: holder,
            connectivity_score: score,
            renewed_at: Hlc::zero(),
        }
    }

    #[test]
    fn higher_epoch_always_wins_even_with_a_worse_score() {
        let old = lease(1, "alice", 100.0);
        let new = lease(2, "bob", 1.0);
        assert_eq!(merge_lease(old, new).holder_pubkey, "bob");
        assert_eq!(merge_lease(new, old).holder_pubkey, "bob", "merge must be commutative");
    }

    #[test]
    fn same_epoch_best_connectivity_wins() {
        let weak = lease(1, "alice", 5.0);
        let strong = lease(1, "bob", 50.0);
        assert_eq!(merge_lease(weak, strong).holder_pubkey, "bob");
        assert_eq!(merge_lease(strong, weak).holder_pubkey, "bob");
    }

    #[test]
    fn ties_break_deterministically_by_pubkey() {
        let a = lease(1, "alice", 10.0);
        let b = lease(1, "bob", 10.0);
        assert_eq!(merge_lease(a, b).holder_pubkey, "alice");
        assert_eq!(merge_lease(b, a).holder_pubkey, "alice", "must be order-independent");
    }
}
