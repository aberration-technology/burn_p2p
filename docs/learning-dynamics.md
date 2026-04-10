# learning dynamics

this note explains what `burn_p2p` is optimizing today, why the model can
still learn under periodic p2p averaging, and where the current design differs
from classic synchronized distributed training.

it also gives one small, concrete starting profile for a decentralized native
experiment.

## the current training loop

one training lease is one micro-epoch.

the runtime currently does this:

1. trainers sync the latest visible canonical head
2. each trainer runs one local train window on its leased micro-epoch
3. the trainer publishes an update, receipt, and model artifact
4. reducers gather eligible updates into one aggregate proposal
5. validators screen candidates, merge accepted updates, and promote a merged head
6. trainers move to the next window, or reconcile with the newer canonical head if
   they are using the continuous trainer path

that is much closer to local sgd / fedavg style periodic averaging than to
global all-reduce-per-step training.

## how updates are weighted

the short answer is: no, contributions are not always weighted equally.

there are three layers of weighting in the current design:

1. base contribution weight
   `P2pWorkload::contribution_weight(...)` is the workload hook that defines the
   raw weight of a completed train window. for burn and python workloads, the
   default now follows standard processed-work metrics when they are present:
   `tokens_processed`, then `examples_processed`, then smaller fallbacks such as
   `sample_count` or `batch_count`. if none of those are present, the fallback
   is still `1.0`.

2. trainer quality weight
   when a trainer publishes an update, the runtime derives a `quality_weight`
   from the reported metrics:

   `quality_weight = max(0.01, 1 / (1 + abs(metric_quality)))`

   in the built-in metric mapping, lower loss or higher accuracy yields a
   larger quality weight.

3. robustness scaling
   validators can reject or downweight candidates before merge. that stage
   scales the effective sample weight and can also cap browser contributions or
   clip outlier updates.

for the burn adapter, the current effective merge weight is:

`w_i = contribution_weight_i * robustness_scale_i * trainer_quality_weight_i`

if a workload does not expose processed-work metrics and also leaves
`contribution_weight(...)` unchanged, then all accepted windows start equal,
but they still diverge once quality weighting and robustness filtering are
applied.

same-peer speculative chains are also collapsed before merge. when that
happens, sample weights add, and quality weight becomes the sample-weighted
average across the chain.

## why weight averaging can still learn

the design works best when all local windows stay close enough to the same base
head that their updates remain in a compatible basin.

that is the same intuition behind several known families of methods:

- [fedavg](https://arxiv.org/abs/1602.05629): periodic model averaging after
  local work
- [local sgd](https://arxiv.org/abs/1805.09767): multiple local steps with less
  frequent communication
- [easgd](https://arxiv.org/abs/1412.6651): allowing local workers to drift
  while softly pulling them back to a center
- [swa](https://arxiv.org/abs/1803.05407): weight averaging can favor flatter,
  more stable solutions
- [model soups](https://arxiv.org/abs/2203.05482): averaging works well when
  models stay in a shared low-error basin

in practice, `burn_p2p` relies on the same conditions:

- the experiment stays on one revision with one compatible model schema
- micro-epochs are short enough that local drift stays bounded
- reducers/validators close windows often enough that stale heads do not build
  up for too long
- the data distribution is not so hostile that local windows point in totally
  different directions

the continuous trainer path helps keep trainers busy between promotions. by
default it uses a root-ema reconcile step when a newer canonical head becomes
visible:

- canonical weight: `0.25`
- retained local weight: `0.75`
- speculative lead cap: `2` windows
- visibility wait: `750 ms`

that is a pragmatic stability choice: trainers do not throw away all local
progress immediately, but they do get pulled back toward the promoted head.

## how this differs from classic sync all-reduce

classic distributed data parallel training usually:

- averages gradients every step or every very small number of steps
- keeps optimizer state tightly synchronized
- makes every worker advance from almost the same parameter state
- behaves much more like one large batch sgd process

the current `burn_p2p` design instead:

- averages model updates after a full micro-epoch
- allows reducers and validators to close windows asynchronously
- does not synchronize optimizer state across peers
- can let trainers get ahead of the latest visible canonical head
- optionally applies ema-style reconcile instead of hard replacement

that makes it cheaper on communication and much easier to operate in a mixed,
partially connected, or browser-inclusive network, but it also introduces real
limitations.

## main limitations

compared with classic sync all-reduce, the current paradigm has a few important
failure modes:

- more drift under non-iid data
  fedavg-style methods are known to suffer when client data is highly
  heterogeneous. [fedprox](https://arxiv.org/abs/1812.06127) and
  [scaffold](https://arxiv.org/abs/1910.06378) both target this exact problem.

- stale updates
  reducers and validators merge after the fact, so a trainer may keep working
  from an older canonical head for longer than a sync-sgd worker would.

- no shared optimizer state
  momentum or adaptive optimizer statistics stay local. the network only merges
  model state, not the full optimizer trajectory.

- window sizing matters a lot
  if micro-epochs are too large, peers wander too far apart before averaging.
  if they are too small, communication and validation dominate.

- robustness can change the optimization geometry
  clipping, downweighting, and cohort filtering improve safety, but they also
  mean the final merged model is not just a plain arithmetic average.

so the current design should be thought of as a decentralized, windowed,
model-averaging system. it is not an exact substitute for tightly synchronized
ddp.

## the current default shape

these are the main defaults baked into the runtime today:

- merge strategy: `ReplicatedRendezvousDag`
- reducer replication: `2`
- target leaf cohort: `16`
- upper fan-in: `4`
- merge window duration: `300 s`
- publish jitter: `750 ms`
- staleness window tolerance: `2`
- validator quorum: `2`

`window_duration_secs` is the merge-window close budget on the control plane.
it is not the target per-peer train time. in healthy runs, local train windows
should usually finish well before that budget is exhausted.

the default robustness profile starts with:

- global norm cap: `8.0`
- minimum cosine similarity: `0.05`
- trim fraction: `0.1`
- minimum survivor count: `2`

two important notes:

- validator merge currently uses `MergePolicy::QualityWeightedEma`
- for burn workloads, that means accepted candidates are merged with weighted
  mean using `sample_weight * quality_weight`

the optional post-merge root ema is separate. the runtime promotion policy asks
for it by default, but the burn adapter only applies it when the workload is
configured with `.with_root_ema(decay)`.

## a sane starting profile

for a small native decentralized experiment with modest trust and moderate peer
churn, the current design is most coherent when used like this:

- keep micro-epochs short
  aim for a local train window that is long enough to keep the device busy, but
  short enough that peers still remain close to the same base head.

- weight by examples or tokens when peer work is uneven
  if one peer processes 4x as many examples as another, equal weighting is
  usually wrong. the burn and python adapters now pick up standard
  processed-work counters by default, but override `contribution_weight(...)`
  if your workload needs a different notion of work.

- keep speculative lead small
  the default `max_speculative_windows = 2` is a reasonable starting point.
  going much larger without a stronger correction mechanism can increase drift.

- keep validator quorum small unless the trust model demands more
  `validator_quorum = 2` is a practical latency default for small experiments.
  raising quorum improves trust but increases close latency.

- start with the current topology defaults
  `target_leaf_cohort = 16`, `upper_fanin = 4`, and `reducer_replication = 2`
  are a reasonable middle ground between fan-in, reducer load, and resilience.

- enable root ema when the experiment is noisy
  if promotions are visibly jumpy, enabling `.with_root_ema(decay)` on the burn
  workload can damp sharp transitions at the cost of slower adaptation.

- do not treat the current defaults as llm-scale tuning
  for very large models or very heterogeneous data, you should expect to retune
  micro-epoch size, contribution weighting, and reconcile behavior.

## recommended experiment profiles

### profile 1: small mixed-trust native training

use this when:

- all peers are native
- links are reasonably good
- the trust model is not fully adversarial
- the goal is to stay fairly close to the latest canonical head

starting shape:

- keep local train windows short and regular
- keep `validator_quorum = 2`
- keep `reducer_replication = 2`
- keep `target_leaf_cohort = 16`
- keep `upper_fanin = 4`
- keep continuous trainer reconcile at the default `canonical_weight = 0.25`
- keep speculative lead capped at `2` windows
- start with workload `contribution_weight(...)` equal to examples or tokens if
  peer work is materially uneven

why this profile works:

- canonical visibility stays fairly frequent
- local drift stays bounded
- validator close latency stays low enough that trainers do not run too far
  ahead
- the system behaves like a practical local-sgd / fedavg-style network without
  paying per-step all-reduce costs

main watch-outs:

- if windows are too large, convergence gets noisier fast
- if one peer does much more work, equal weighting is usually wrong
- if promotions look jumpy, add burn-side `.with_root_ema(decay)`

### profile 2: larger high-latency decentralized training

use this when:

- peer count is larger
- network latency or churn is meaningfully higher
- data heterogeneity is stronger
- visibility lag is expected, not exceptional

starting shape:

- still keep micro-epochs bounded, but tune them from measured drift rather
  than aiming for the shortest possible publish cadence
- override `contribution_weight(...)` so it reflects real examples, tokens, or
  another stable work unit when the default processed-work counters are not the
  right measure
- consider higher `reducer_replication` if reducer loss or churn is realistic
- consider higher `validator_quorum` only when the trust model needs it, and
  accept the extra close latency
- keep speculative lead conservative until the experiment has real drift data
- treat robustness settings as active optimization knobs, not just safety
  defaults

why this profile is harder:

- stale updates are more common
- peers finish windows at different times
- non-iid drift is much more visible
- local optimizer state diverges more between peers

main watch-outs:

- this is where fedavg-style drift really starts to matter
- plain equal weighting gets worse as peer throughput diverges
- aggressive speculative lead can make canonical reconcile too weak
- this profile may need stronger correction methods than the current default
  root-ema reconcile if you push scale or heterogeneity much further

## practical reading of the current design

the design is intentionally opinionated:

- micro-epochs define when checkpoints are emitted
- reducers and validators define when a canonical head becomes visible
- trainers can stay warm between promotions, but should not drift too far
- merge weights should reflect both amount of work and observed update quality

that is why the current system emphasizes:

- frequent enough canonical visibility
- bounded speculative lead
- weighted averaging instead of blind equal averaging
- robustness filters before promotion

## monitoring diffusion and fragmentation

the repo now exposes a small set of metrics that are useful for checkpoint
diffusion and canonical fragmentation:

- `MeanHeadLag` and `MaxHeadLag`
  how stale peer windows are relative to the latest visible canonical head

- `HeadAdoptionLagP50` and `HeadAdoptionLagP90`
  how long it takes peers to start work from a newly certified canonical head

- `LatestCanonicalAdoptionCoverage`
  fraction of recent peer windows that already started from the latest
  canonical head

- `RecentBaseHeadFragmentation`
  number of distinct base heads still visible in recent peer windows after the
  latest canonical promotion

- `CanonicalHeadAdoptionCurve`
  per-head cumulative diffusion curve over that head's own visibility interval,
  so you can see how quickly peers start work from it before the next canonical
  promotion lands

- `VisibleHeadPopulationHistogram`
  latest visible peer population grouped by base head after the newest
  canonical promotion, so you can see whether peers are actually converging on
  one head or still split across stale bases

- `CandidateBranchFactor`
  number of distinct candidate heads alive for one base head before final
  promotion

- `MergeWindowSkew` and `ReducerReplicaAgreement`
  whether reducer closure and reducer replica consistency are contributing to
  slow or fragmented promotion

if you want exact per-step equivalence to classic data-parallel training, this
is the wrong paradigm. if you want a network that can keep learning with
partial connectivity, heterogeneous peers, and less frequent synchronization,
this is the right family of ideas.

## papers worth reading

- [communication-efficient learning of deep networks from decentralized data](https://arxiv.org/abs/1602.05629)
- [local sgd converges fast and communicates little](https://arxiv.org/abs/1805.09767)
- [deep learning with elastic averaging sgd](https://arxiv.org/abs/1412.6651)
- [averaging weights leads to wider optima and better generalization](https://arxiv.org/abs/1803.05407)
- [model soups: averaging weights of multiple fine-tuned models improves accuracy without increasing inference time](https://arxiv.org/abs/2203.05482)
- [federated optimization in heterogeneous networks](https://arxiv.org/abs/1812.06127)
- [scaffold: stochastic controlled averaging for federated learning](https://arxiv.org/abs/1910.06378)
