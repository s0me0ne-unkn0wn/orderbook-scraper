use std::collections::{BTreeSet, HashMap};

pub const MIN_CONNECTIONS: usize = 2;

pub struct Assignments {
    // Maximum number of markets per socket.
    max_per_socket: usize,
    // Market tickers by socket index.
    shards: Vec<BTreeSet<String>>,
    // Socket index by market ticker.
    owners: HashMap<String, usize>,
}

impl Assignments {
    pub fn new(max_per_socket: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(max_per_socket > 0, "markets per websocket must be positive");
        Ok(Self {
            max_per_socket,
            shards: vec![BTreeSet::new(); MIN_CONNECTIONS],
            owners: HashMap::new(),
        })
    }

    pub fn reconcile(&mut self, desired: &BTreeSet<String>) -> Vec<BTreeSet<String>> {
        self.owners.retain(|ticker, _| desired.contains(ticker));
        for shard in &mut self.shards {
            shard.retain(|ticker| desired.contains(ticker));
        }
        let socket_count = desired.len().div_ceil(self.max_per_socket);
        self.shards
            .resize_with(socket_count.max(self.shards.len()), BTreeSet::new);
        let mut loads: BTreeSet<_> = self
            .shards
            .iter()
            .enumerate()
            .map(|(index, shard)| (shard.len(), index))
            .collect();
        for ticker in desired {
            if self.owners.contains_key(ticker) {
                continue;
            }
            let (count, index) = loads.pop_first().expect("no websocket assignments");
            self.shards[index].insert(ticker.clone());
            self.owners.insert(ticker.clone(), index);
            loads.insert((count + 1, index));
        }
        // Rebalance sockets: move one by one from the most loaded to the least loaded until balanced
        loop {
            let &(minimum, destination) = loads.first().expect("no websocket assignments");
            let &(maximum, source) = loads.last().expect("no websocket assignments");
            if maximum - minimum <= 1 {
                break;
            }
            loads.remove(&(minimum, destination));
            loads.remove(&(maximum, source));
            let ticker = self.shards[source]
                .pop_first()
                .expect("empty source websocket");
            self.owners.insert(ticker.clone(), destination);
            self.shards[destination].insert(ticker);
            loads.insert((minimum + 1, destination));
            loads.insert((maximum - 1, source));
        }
        self.shards.clone()
    }

    pub fn owner(&self, ticker: &str) -> Option<usize> {
        self.owners.get(ticker).copied()
    }

    pub fn markets(&self, socket: usize) -> &BTreeSet<String> {
        &self.shards[socket]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn markets(count: usize) -> BTreeSet<String> {
        (0..count)
            .map(|index| format!("MARKET-{index:05}"))
            .collect()
    }

    fn assert_coverage(assignments: &[BTreeSet<String>], desired: &BTreeSet<String>, cap: usize) {
        let union: BTreeSet<_> = assignments.iter().flatten().cloned().collect();
        assert_eq!(&union, desired);
        assert_eq!(
            assignments.iter().map(BTreeSet::len).sum::<usize>(),
            desired.len()
        );
        let counts: Vec<_> = assignments.iter().map(BTreeSet::len).collect();
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
        assert!(counts.iter().all(|count| *count <= cap));
    }

    #[test]
    fn large_market_universe_has_complete_unique_balanced_coverage_with_socket_caps() {
        let mut planner = Assignments::new(300).unwrap();
        let desired = markets(10_001);
        let assignments = planner.reconcile(&desired);
        assert_eq!(assignments.len(), 34);
        assert_coverage(&assignments, &desired, 300);
        for (index, shard) in assignments.iter().enumerate() {
            for ticker in shard {
                assert_eq!(planner.owner(ticker), Some(index));
            }
        }
    }

    #[test]
    fn unchanged_universe_does_not_interrupt_subscriptions() {
        let mut planner = Assignments::new(200).unwrap();
        let desired = markets(999);
        let first = planner.reconcile(&desired);
        assert_eq!(planner.reconcile(&desired), first);
        let mut second_planner = Assignments::new(200).unwrap();
        assert_eq!(second_planner.reconcile(&desired), first);
    }

    #[test]
    fn new_markets_fill_vacancies_without_moving_active_subscriptions() {
        let mut planner = Assignments::new(10).unwrap();
        let first = planner.reconcile(&markets(40));
        let mut desired = markets(40);
        for ticker in first[0].iter().take(5) {
            desired.remove(ticker);
        }
        desired.extend((40..45).map(|index| format!("MARKET-{index:05}")));
        let next = planner.reconcile(&desired);
        assert_coverage(&next, &desired, 10);
        for (index, shard) in first.iter().enumerate() {
            for ticker in shard.intersection(&desired) {
                assert!(next[index].contains(ticker));
            }
        }
    }

    #[test]
    fn asymmetric_expiry_moves_only_markets_necessary_to_restore_balance() {
        let mut planner = Assignments::new(10).unwrap();
        let first = planner.reconcile(&markets(40));
        let desired: BTreeSet<_> = first.iter().skip(1).flatten().cloned().collect();
        let next = planner.reconcile(&desired);
        assert_coverage(&next, &desired, 10);
        let moves = first
            .iter()
            .zip(&next)
            .map(|(previous, current)| {
                previous
                    .intersection(&desired)
                    .filter(|ticker| !current.contains(*ticker))
                    .count()
            })
            .sum::<usize>();
        assert_eq!(moves, 7);
    }

    #[test]
    fn growth_then_expiry_preserves_shard_indices_and_supports_empty_universe() {
        let mut planner = Assignments::new(3).unwrap();
        let first = planner.reconcile(&markets(13));
        assert_eq!(first.len(), 5);
        let desired = markets(2);
        let next = planner.reconcile(&desired);
        assert_eq!(next.len(), first.len());
        assert_coverage(&next, &desired, 3);
        let empty = planner.reconcile(&BTreeSet::new());
        assert_eq!(empty.len(), first.len());
        assert!(empty.iter().all(BTreeSet::is_empty));
        assert_eq!(planner.owner("MARKET-00000"), None);
    }

    #[test]
    fn market_count_sets_socket_count_with_a_minimum_of_two() {
        for (count, sockets) in [(0, 2), (1, 2), (2, 2), (6, 2), (7, 3)] {
            let mut planner = Assignments::new(3).unwrap();
            let desired = markets(count);
            let assignments = planner.reconcile(&desired);
            assert_eq!(assignments.len(), sockets);
            assert_coverage(&assignments, &desired, 3);
        }
    }

    #[test]
    fn zero_capacity_cannot_form_assignments() {
        assert!(Assignments::new(0).is_err());
    }
}
