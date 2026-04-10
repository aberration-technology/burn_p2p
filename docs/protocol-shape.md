# protocol shape

this note describes the authority boundary in `burn_p2p`.

the short version:

- trainers publish candidate updates
- reducers build aggregate proposals
- validators decide what becomes canonical

reducers run before validators in the happy path, but that is a performance
optimization, not the trust boundary.

## what is authoritative

the protocol has a deliberate order:

1. trainers publish update artifacts and receipts for one micro-epoch
2. reducers may publish an aggregate proposal for that candidate cohort
3. validators independently rescreen the cohort, rebuild the expected merge
   inputs, and attest the reduction they are willing to stand behind
4. validator quorum publishes the validation quorum and merge certificates
5. the merged head becomes canonical

the important point is that step 2 is speculative, while steps 3 to 5 are
authoritative.

## why reducers are not enough

a reducer proposal is useful because it can:

- cut duplicate merge work
- make aggregate artifacts available earlier
- shrink close latency when the reducer is healthy

but a reducer proposal is not trusted blindly.

validators still:

- recompute candidate eligibility locally
- apply robustness screening and canary checks locally
- derive the expected aggregate record locally
- require validator quorum before canonical promotion

if a dedicated reducer is missing, late, or serves a mismatched aggregate,
validators now fall back to local reduction and continue. that keeps reducers
as accelerators instead of a liveness choke point.

## what adversarial peers can still do

the current protocol is designed to keep bad peers from directly poisoning
canonical state, but it does not make them free.

bad trainers can:

- publish junk updates
- try to replay receipts
- try to push outlier deltas

the validator path answers that with:

- duplicate/replay rejection
- robustness screening and downweighting
- canary-based promotion blocking
- quarantine and future eligibility loss

bad reducers can:

- publish a wrong aggregate proposal
- withhold an aggregate proposal
- waste sync time and bandwidth

the validator path answers that with:

- independent local screening and merge reconstruction
- semantic aggregate verification, not just proposal observation
- local reduction fallback when remote reducer output is unusable

reducer-only abuse is therefore mainly an availability problem. today the
automatic quarantine path is strongest on bad trainer updates; persistent bad
reducer or validator behavior should still be answered with role removal,
admission policy, or explicit revocation.

so a bad reducer should not be able to advance a wrong canonical head on its
own. the cost is mostly availability and extra work, not silent corruption.

## where the real trust boundary is

the real authority boundary is the validator set and its quorum size.

that means:

- `validator_quorum = 1` is only appropriate for local or fully trusted runs
- for any semi-trusted deployment, use more than one validator
- validators should be operated separately from normal trainer pools
- reducers should be treated as replaceable helpers, not authorities

this protocol is closer to validator-quorum promotion on top of decentralized
training than to a general-purpose byzantine consensus system.

if validator quorum is compromised, canonical safety is compromised too.

## recommended operating shape

for the current design, the most coherent deployment shape is:

- many trainers
- optional dedicated reducers
- a small, separately managed validator set
- admission policy enabled for any untrusted or public membership
- short micro-epochs so validator close stays cheap enough to run often

that keeps the protocol aligned with what it is good at:

- decentralized training with bounded drift
- validator-gated canonical promotion
- mixed native/browser fleets when needed

and avoids pretending it is something else:

- step-synchronous all-reduce
- or a full bft replication layer
