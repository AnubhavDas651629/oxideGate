//! Deficit round robin across tenants (ROADMAP D6).
//!
//! Each tenant with work waiting has its own FIFO queue. Tenants take turns
//! in a rotation. At the start of its turn a tenant earns `weight` credit;
//! each dispatch costs 1; it keeps being served while it has >= 1 credit,
//! then the turn passes on. With paid = 1.0 and free = 0.5, a backlogged
//! paid tenant is served once per turn and a free tenant once every two,
//! so under contention paid gets 2 dispatches for each free one.
//!
//! What this buys: a free-tier flood grows the free tenant's own queue,
//! not the line a paid request has to stand in. A paid request waits for
//! at most one turn of each other active tenant, however deep their
//! backlogs are.
//!
//! Plain data structure, no locking or async: the scheduler wraps it in a
//! Mutex. That keeps the algorithm unit-testable on its own.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

struct TenantQueue<T> {
    items: VecDeque<T>,
    weight: f64,
    deficit: f64,
}

pub struct FairQueue<T> {
    queues: HashMap<Arc<str>, TenantQueue<T>>,
    /// Tenants with something queued, in rotation order. Front = whose turn.
    active: VecDeque<Arc<str>>,
    /// Whether the tenant at the front has already received this turn's
    /// credit. Needed because the scheduler pops one item at a time, so a
    /// turn can span several pop() calls.
    turn_started: bool,
    len: usize,
    capacity: usize,
}

impl<T> FairQueue<T> {
    pub fn new(capacity: usize) -> Self {
        FairQueue {
            queues: HashMap::new(),
            active: VecDeque::new(),
            turn_started: false,
            len: 0,
            capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Enqueue for `key`. Hands the item back if the queue is at capacity,
    /// so the caller can turn it into a 429.
    pub fn push(&mut self, key: &Arc<str>, weight: f64, item: T) -> Result<(), T> {
        if self.len >= self.capacity {
            return Err(item);
        }
        self.len += 1;
        match self.queues.get_mut(key) {
            Some(q) => q.items.push_back(item),
            None => {
                // New (or newly active) tenant joins at the back of the
                // rotation with no credit carried over.
                self.queues.insert(
                    Arc::clone(key),
                    TenantQueue {
                        items: VecDeque::from([item]),
                        weight,
                        deficit: 0.0,
                    },
                );
                self.active.push_back(Arc::clone(key));
            }
        }
        Ok(())
    }

    /// Next item by DRR order.
    pub fn pop(&mut self) -> Option<T> {
        // Terminates: every active tenant has a non-empty queue and a
        // positive weight, so some tenant reaches 1 credit within
        // ceil(1 / smallest weight) full rotations.
        loop {
            let key = self.active.front()?;
            let q = self.queues.get_mut(key)?;

            if !self.turn_started {
                q.deficit += q.weight;
                self.turn_started = true;
            }

            if q.deficit >= 1.0 {
                q.deficit -= 1.0;
                let item = q.items.pop_front();
                self.len -= 1;

                if q.items.is_empty() {
                    // Standard DRR: an idle tenant keeps no credit, or it
                    // could save up and burst past everyone later.
                    let key = self.active.pop_front();
                    if let Some(k) = key {
                        self.queues.remove(&k);
                    }
                    self.turn_started = false;
                } else if q.deficit < 1.0 {
                    self.active.rotate_left(1);
                    self.turn_started = false;
                }
                return item;
            }

            // Not enough credit this turn: pass to the next tenant.
            self.active.rotate_left(1);
            self.turn_started = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    #[test]
    fn single_tenant_is_fifo() {
        let mut q = FairQueue::new(10);
        let a = key("a");
        for i in 0..5 {
            q.push(&a, 1.0, i).unwrap();
        }
        let out: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(out, [0, 1, 2, 3, 4]);
        assert!(q.is_empty());
    }

    #[test]
    fn capacity_is_global_and_returns_the_item() {
        let mut q = FairQueue::new(2);
        q.push(&key("a"), 1.0, 1).unwrap();
        q.push(&key("b"), 1.0, 2).unwrap();
        assert_eq!(q.push(&key("c"), 1.0, 3), Err(3));
        q.pop();
        assert!(q.push(&key("c"), 1.0, 3).is_ok());
    }

    #[test]
    fn backlogged_paid_gets_twice_the_dispatches_of_free() {
        let mut q = FairQueue::new(1000);
        let (paid, free) = (key("paid"), key("free"));
        for _ in 0..300 {
            q.push(&free, 0.5, "free").unwrap();
            q.push(&paid, 1.0, "paid").unwrap();
        }
        let first_300: Vec<_> = (0..300).map(|_| q.pop().unwrap()).collect();
        let paid_n = first_300.iter().filter(|&&s| s == "paid").count();
        assert_eq!(
            paid_n, 200,
            "expected a 2:1 split, got {paid_n} paid of 300"
        );
    }

    #[test]
    fn late_paid_request_does_not_wait_behind_a_free_flood() {
        let mut q = FairQueue::new(1000);
        let (paid, free) = (key("paid"), key("free"));
        for i in 0..500 {
            q.push(&free, 0.5, i).unwrap();
        }
        q.pop(); // scheduler has started working through the flood
        q.push(&paid, 1.0, 9999).unwrap();

        // Within a few pops the paid request must come out — not after 499.
        let position = (1..=5).find(|_| q.pop() == Some(9999));
        assert!(position.is_some(), "paid request stuck behind the flood");
    }

    #[test]
    fn idle_tenant_does_not_bank_credit() {
        let mut q = FairQueue::new(100);
        let (a, b) = (key("a"), key("b"));
        // a is served alone for a while, then goes idle.
        for _ in 0..10 {
            q.push(&a, 1.0, "a").unwrap();
        }
        while q.pop().is_some() {}
        // Now both backlogged: a must not get a burst from past turns.
        for _ in 0..10 {
            q.push(&b, 1.0, "b").unwrap();
            q.push(&a, 1.0, "a").unwrap();
        }
        let first4: Vec<_> = (0..4).map(|_| q.pop().unwrap()).collect();
        assert_eq!(first4.iter().filter(|&&s| s == "a").count(), 2);
    }

    #[test]
    fn free_alone_is_still_served_every_pop() {
        // Weight < 1 only matters relative to others; alone, a free tenant
        // must not be slowed down.
        let mut q = FairQueue::new(10);
        let f = key("f");
        for i in 0..4 {
            q.push(&f, 0.5, i).unwrap();
        }
        let out: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(out, [0, 1, 2, 3]);
    }
}
