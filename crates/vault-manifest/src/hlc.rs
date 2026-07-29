//! Hybrid Logical Clock: orders events across agents without synchronized wall clocks.
//! See ARCHITECTURE.md §2.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hlc {
    pub physical_ms: u64,
    pub logical: u32,
}

impl Hlc {
    pub fn zero() -> Self {
        Hlc { physical_ms: 0, logical: 0 }
    }

    /// Advance the clock for a local event, given the current wall-clock reading.
    pub fn tick(&mut self, wall_ms: u64) -> Hlc {
        if wall_ms > self.physical_ms {
            self.physical_ms = wall_ms;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        *self
    }

    /// Merge in a remote timestamp on message receipt, given the current wall-clock reading.
    pub fn observe(&mut self, remote: Hlc, wall_ms: u64) -> Hlc {
        let max_physical = wall_ms.max(self.physical_ms).max(remote.physical_ms);
        if max_physical == self.physical_ms && max_physical == remote.physical_ms {
            self.logical = self.logical.max(remote.logical) + 1;
        } else if max_physical == self.physical_ms {
            self.logical += 1;
        } else if max_physical == remote.physical_ms {
            self.logical = remote.logical + 1;
        } else {
            self.logical = 0;
        }
        self.physical_ms = max_physical;
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_advance_logical_when_wall_clock_stalls() {
        let mut c = Hlc::zero();
        let a = c.tick(100);
        let b = c.tick(100);
        assert_eq!(a.physical_ms, 100);
        assert_eq!(b.physical_ms, 100);
        assert!(b.logical > a.logical);
    }

    #[test]
    fn observe_takes_the_max_and_stays_strictly_increasing() {
        let mut local = Hlc::zero();
        local.tick(50);
        let remote = Hlc { physical_ms: 200, logical: 3 };
        let merged = local.observe(remote, 60);
        assert_eq!(merged.physical_ms, 200);
        assert_eq!(merged.logical, 4);
    }
}
