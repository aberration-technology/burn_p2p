# burn_p2p Dataloader Tuning

## Current defaults

Planner defaults reviewed from the current codebase:

- `target_microshard_bytes`: 64 MiB
- `min_microshards`: 1
- `max_microshards`: 16,384
- `max_microshards_per_lease`: 128

## Tuning goals

Balance:

- lease startup latency
- browser cache pressure
- checkpoint/update overlap
- storage seek overhead
- reducer fairness

## Practical guidance

### Use larger microshards when

- the deployment is native-only
- storage is fast and local
- the dataset is relatively uniform
- browser peers are not expected to fetch those shards

### Use smaller microshards when

- browser peers are expected to participate
- shard sizes are highly skewed
- resume/catch-up latency matters more than peak throughput
- the workload fetches only small working sets per window

## `max_microshards_per_lease`

This setting controls how many microshards a single lease can point at.

Increase it when:

- the workload naturally consumes many small shards
- fetch latency is low
- per-window scheduling overhead dominates

Decrease it when:

- browser peers run out of persistence or memory budget
- lease startup spends too long warming caches
- fetch and eviction churn dominates runtime time

## Browser guidance

For browser-enabled revisions:

- keep microshards small enough to fit the browser shard budget
- validate against `browser_target_window_seconds`
- confirm suspension/resume does not require re-fetching an excessive shard set

## Recommended operator process

1. benchmark planning with realistic dataset sizes
2. benchmark fetch with skewed shard distributions
3. verify browser cache pressure and resume cost
4. adjust `target_microshard_bytes`
5. adjust `max_microshards_per_lease`
6. rerun join-to-window and catch-up measurements
