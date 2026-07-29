//! Local, in-process simulation of N "agents" gossiping manifest updates and a
//! provider-lease takeover — no real networking. This is the MVP test-plan step
//! from ARCHITECTURE.md §7 ("single-host simulation ... verify concurrent writes
//! ... converge ... killing the provider triggers a clean takeover").

use vault_manifest::hlc::Hlc;
use vault_manifest::lease::{connectivity_score, merge_lease, ProviderLease};
use vault_manifest::manifest::{Entry, Manifest};

struct Agent {
    name: &'static str,
    clock: Hlc,
    manifest: Manifest,
}

impl Agent {
    fn new(name: &'static str) -> Self {
        Agent { name, clock: Hlc::zero(), manifest: Manifest::new() }
    }

    fn write(&mut self, path: &str, blob_hash: &str, wall_ms: u64) {
        let hlc = self.clock.tick(wall_ms);
        self.manifest.put(
            path,
            Entry {
                blob_hash: blob_hash.to_string(),
                size: blob_hash.len() as u64,
                hlc,
                author_pubkey: self.name.to_string(),
                tombstone: false,
            },
        );
    }

    fn gossip_from(&mut self, other: &Agent) {
        self.manifest.merge(&other.manifest);
    }
}

#[test]
fn three_agents_converge_after_gossiping_pairwise() {
    let mut alice = Agent::new("alice");
    let mut bob = Agent::new("bob");
    let mut carol = Agent::new("carol");

    // Alice and Bob write to different paths concurrently; Carol writes a later
    // update to Alice's path.
    alice.write("notes/a.md", "hash-a1", 100);
    bob.write("notes/b.md", "hash-b1", 100);
    carol.write("notes/a.md", "hash-a2", 200); // later HLC, should win over alice's

    // Full pairwise gossip round (every agent pulls from every other).
    let snapshots = [alice.manifest.clone(), bob.manifest.clone(), carol.manifest.clone()];
    for agent in [&mut alice, &mut bob, &mut carol] {
        for snap in &snapshots {
            agent.manifest.merge(snap);
        }
    }

    for agent in [&alice, &bob, &carol] {
        assert_eq!(
            agent.manifest.get("notes/a.md").unwrap().blob_hash,
            "hash-a2",
            "{} should see Carol's later write win",
            agent.name
        );
        assert_eq!(agent.manifest.get("notes/b.md").unwrap().blob_hash, "hash-b1");
        assert_eq!(agent.manifest.len(), 2, "{} manifest size", agent.name);
    }
}

#[test]
fn concurrent_writes_to_the_same_path_are_both_preserved_after_gossip() {
    let mut alice = Agent::new("alice");
    let mut bob = Agent::new("bob");

    // Same wall-clock tick on both, no prior communication -> truly concurrent.
    alice.write("shared.txt", "alice-version", 500);
    bob.write("shared.txt", "bob-version", 500);

    alice.gossip_from(&bob);
    bob.gossip_from(&alice);

    // Neither replica may silently drop a write: exactly one entry lives at the
    // canonical path, the other survives as a conflict copy, and both replicas
    // agree on which is which.
    assert_eq!(alice.manifest.len(), 2);
    assert_eq!(bob.manifest.len(), 2);
    let alice_winner = alice.manifest.get("shared.txt").unwrap().blob_hash.clone();
    let bob_winner = bob.manifest.get("shared.txt").unwrap().blob_hash.clone();
    assert_eq!(alice_winner, bob_winner, "both replicas must pick the same winner");
}

#[test]
fn provider_lease_takeover_prefers_the_best_connected_survivor() {
    // Simulates: alice was the provider (epoch 1), goes silent (dies / drops).
    // Bob and Carol both notice and independently propose a takeover in the
    // same window; every node must converge to the SAME new provider without
    // a coordinator, purely from the merge rule.
    let hlc = Hlc::zero();
    let alice_lease = ProviderLease {
        epoch: 1,
        holder_pubkey: "alice",
        connectivity_score: connectivity_score(4, 20.0),
        renewed_at: hlc,
    };

    let bob_proposal = ProviderLease {
        epoch: 2,
        holder_pubkey: "bob",
        connectivity_score: connectivity_score(2, 15.0), // 2*10 - 15 = 5.0
        renewed_at: hlc,
    };
    let carol_proposal = ProviderLease {
        epoch: 2,
        holder_pubkey: "carol",
        connectivity_score: connectivity_score(5, 10.0), // 5*10 - 10 = 40.0
        renewed_at: hlc,
    };

    // Every node merges the same three observed leases, in different arrival
    // orders, and must land on the same winner: carol (higher epoch than
    // alice's stale lease, and better-connected than bob within epoch 2).
    let order_a = merge_lease(merge_lease(alice_lease, bob_proposal), carol_proposal);
    let order_b = merge_lease(merge_lease(carol_proposal, alice_lease), bob_proposal);
    let order_c = merge_lease(merge_lease(bob_proposal, carol_proposal), alice_lease);

    assert_eq!(order_a.holder_pubkey, "carol");
    assert_eq!(order_b.holder_pubkey, "carol");
    assert_eq!(order_c.holder_pubkey, "carol");
}
