# vsm-agent-kernel

A Rust kernel for a recursive Viable System Model (VSM) coding-agent organization whose structure can evolve as an organizational genome.

The project models an autonomous coding organization as recursive viable nodes, not as a flat worker pool. Internal nodes are metasystems that decompose, route, coordinate, audit, and allocate resources. Operational leaves execute work according to runtime-derived capabilities.

The core invariant is enforced by node state, not prompt text:

```text
node has children    -> metasystem -> cannot write code through the worker harness
node has no children -> System 1 leaf -> may execute only according to leaf capabilities
```

## Workspace

The workspace is split into six crates:

- **`vsm-core`**: pure domain model. Nodes, capabilities, channels, messages, task packets, genome patches, gene suggestions, traces, fitness scaffolding, and the transport trait.
- **`vsm-runtime`**: in-memory transport and early mutation-trial utilities for local simulation.
- **`vsm-amqp`**: RabbitMQ/AMQP transport adapter implementing `vsm-core::Transport`.
- **`vsm-worker`**: leaf worker harness. It subscribes to task packets, enforces leaf capabilities, calls a model provider, publishes task results, and records task traces.
- **`vsm-ledger`**: storage-agnostic empirical ledger with in-memory and SQLite implementations.
- **`vsm-controller`**: metasystem runtime for directive intake, routing/delegation, result observation, ledger events, and System 3* audit suggestions.

## Conceptual Model

```text
ViableNode
├── System 5: identity, policy, values
├── System 4: future probes and environmental sensing
├── System 3: present-time control, decomposition, resources, integration
├── System 3*: audit and gene suggestion
├── System 2: coordination channels among children
└── System 1:
    ├── child ViableNodes, if non-leaf
    └── leaf operation, if no children
```

A parent-child containment edge creates a standard VSM channel bundle:

- resource bargaining
- command
- System 2 coordination
- System 3* audit
- algedonic upward signaling

The persistent organization is an `OrganizationalGenome`. Genes include nodes, prompts, tools, permissions, VSM channels, relations, routing rules, audit policies, and mutation/trial settings.

## Task Packets

High-level directives are converted into `TaskPacket`s. A task packet is the operational "task genome":

```text
goal
target state
scope
constraints
context refs
authority refs
dependencies
acceptance criteria
risk class
static predicates
assignment metadata
```

The controller currently performs simple directive-to-task mapping and direct-child routing. Rich decomposition and dependency graph planning are future work.

## Core Genome Mutation

The first structural mutation is implemented as a genome patch:

```rust
OrganizationalGenomePatch::PromoteLeafToMetasystem {
    node_id: primary_code_service_id,
    extracted_child_name: "generalist-coder".to_string(),
}
```

This converts:

```text
root
└── primary-code-service [leaf, can write code]
```

into:

```text
root
└── primary-code-service [metasystem, cannot write code]
    └── generalist-coder [leaf, can write code]
```

Adding children changes the node's runtime capabilities. The promoted parent loses code-writing authority, independent of what its prompt says.

## Transport Model

All communication flows through a broker-agnostic trait:

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn publish(&self, envelope: MessageEnvelope) -> Result<(), TransportError>;
    async fn subscribe(&self, subscription: Subscription) -> Result<EnvelopeStream, TransportError>;
}
```

Messages carry VSM channel type, source/target nodes, causation/correlation IDs, priority, payload type, payload JSON, trace path, and metadata.

RabbitMQ is one adapter. The same worker and controller runtimes can use in-memory transport, RabbitMQ, NATS, Kafka, Redis streams, HTTP, local IPC, or any future adapter implementing `Transport`.

## Worker Harness

`vsm-worker::WorkerHarness` wires together:

```text
Transport subscription
        ↓
TaskPacket envelope
        ↓
capability / executable-leaf check
        ↓
TaskPromptBuilder
        ↓
ModelProvider trait
        ↓
TaskResult envelope + TaskTrace
        ↓
TraceSink / LedgerTraceSink
```

The harness depends on traits:

```rust
#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse, ModelProviderError>;
}
```

Included providers:

- `EchoModelProvider`: deterministic local provider for wiring tests.
- `CodexCliProvider`: invokes `codex exec` as a child process.
- `OpenAiCodexProvider`: experimental Responses API adapter retained as an example.

## Ledger

`vsm-ledger` records what happened instead of trying to predict latent value before a mutation exists.

The ledger stores:

- messages received and published
- directives accepted
- task mapping lineage
- tasks routed
- task results received
- task traces written
- algedonic signals
- audit start/completion
- gene suggestions
- genome patch events
- genome snapshots
- per-controller champion genome pointers
- queued, active, and archived trial records

Implementations:

- `InMemoryLedger`
- `SqliteLedger`

The default subtree trace query uses `assigned_node_id` plus `responsible_ancestor_ids`. Task mapping and routing events carry directive IDs, parent task IDs, dependency IDs, source channel metadata, and correlation/causation IDs so decomposition views can be derived from VSM channel traffic instead of stored as a separate authoritative graph. `vsm-core` also exposes minimum viable attributed fitness summaries: direct workers receive direct credit/blame, the immediate responsible parent receives subtree credit/blame, and higher ancestors receive decayed credit/blame. SQLite and in-memory ledgers also persist enough genome/trial state for controller restart recovery and queued candidate activation.

## Controller

`vsm-controller::ControllerRuntime` is the current metasystem runtime.

Current flow:

```text
Directive envelope
  -> ControllerRuntime
  -> DirectiveTaskMapper
  -> TaskRouter
  -> child TaskPacket envelope
  -> WorkerHarness
  -> TaskResult envelope
  -> ControllerRuntime observes result
  -> Ledger records route events and task traces
```

The controller cannot write code. It only routes, delegates, observes, and audits.

The current router is intentionally simple:

- prefer explicit assignment to a direct child
- prefer `target_child` metadata
- otherwise choose the first direct child with required capabilities
- route to a direct child metasystem when deeper delegation is needed

## System 3* Audit

`System3StarAuditor` is the audit extension point. Auditors inspect observed child behavior and generate candidate genes. They do not apply mutations directly.

The first implementation, `RuleBasedSystem3StarAuditor`, is intentionally simple:

```text
if child/subtree failure ratio is high enough
  -> suggest adding a probationary review leaf
```

The audit logs `GeneSuggestionCreated` events. Suggestions can now be admitted into the single-trial controller lifecycle, but audit still does not directly mutate the live genome.

## Mutation Trials

`vsm-runtime::MutationTrial` is the bounded-trial utility:

```text
GeneSuggestion
  -> candidate genome
  -> trial traces
  -> Continue / Promote / Prune decision
```

`vsm-controller` wires this into a single active controller-managed trial:

```text
System 3* audit
  -> GeneSuggestion
  -> candidate genome
  -> durable candidate queue
  -> bounded exposure
  -> historical trace replay
  -> trial-tagged traces
  -> Pareto-front empirical selection
  -> promote, continue, or prune
```

Candidate leaves are executable only when a worker harness is manually registered for the candidate node. The controller logs trial lifecycle events through the ledger, persists queued and active trial state, and promotes by replacing the shared champion genome with the candidate genome. The promoted champion and active trial records can be loaded back from `vsm-ledger`.

Multiple candidates can be queued durably through `queue_candidate_from_suggestion`, but only one queued candidate can be activated at a time through `start_next_queued_trial`. Candidates whose base genome no longer matches the current champion are rejected before activation instead of being evaluated against the wrong baseline. Valid queued candidates are replayed against recent base-genome traces to estimate which historical tasks the candidate would have affected, then filtered to a Pareto frontier with `pareto_empirical_candidate_score_v1` over expected value, safety, historical fit, replay fit, complexity cost, and exposure cost. The remaining frontier candidates use the empirical score as a tie-breaker, combining suggestion source, trial mode, evidence count, explicit selection metadata, safety bounds, realized completed-trial history for matching source/mode/patch kind, replay score, and finally age. Each activation persists a population archive record for every evaluated candidate, marking dominated candidates, Pareto-frontier candidates, and the selected candidate.

Trial routing now respects `trial_mode` and bounded exposure. `Probation` requires explicit approval unless a traffic-share limit is configured, `Canary` uses deterministic task/suggestion hashing against `max_traffic_share_basis_points`, `Direct` can take matching work without approval, and high/critical-risk canary or probation tasks still require explicit approval. `Shadow` mode publishes a non-controlling candidate copy after the champion route is selected; shadow results update trial evidence but are not returned as the controlling task result. Trial-routed task/result/trace metadata includes the mode, route role, and exposure bucket.

Trial promotion/pruning still uses direct trial scores to avoid double-counting parent and child credit in one decision total. `MutationTrial::attributed_fitness` exposes per-node attributed summaries for audits, future subtree pruning, and later population-level selection.

## Current Status

| # | Item | Status |
|---:|---|---|
| 1 | Leaf worker harness | Implemented |
| 2 | Ledger | Implemented, including genome snapshots and trial state |
| 3 | Parent controller | Implemented, basic version |
| 4 | Subtree attribution | Implemented, minimum viable direct/ancestor model |
| 5 | Simple fitness scoring | Implemented, basic direct and attributed summaries |
| 6 | System 3* audit suggestions | Implemented, scaffold/rule-based version |
| 7 | Bounded mutation experiments | Implemented for queued candidates with replay-aware Pareto-front scored activation, durable population/Pareto archive records, one active controller-managed trial, deterministic canary/probation exposure, and shadow duplicate routing |
| 8 | Promotion/pruning loop | Implemented for one active trial; full GA population loop is future work |

The next milestone is richer offline replay/evaluation coverage and model-directed System 3 decomposition through VSM channels. Do not jump to a broad overlapping GA population until attribution and rollback remain reliable under queued candidates.

## Validation

Check the workspace:

```bash
cargo check --workspace
```

Run all compile-level tests:

```bash
cargo test --workspace
```

Run the core genome smoke example:

```bash
cargo run -p vsm-core --example minimal_genome
```

Run the local worker harness simulation:

```bash
cargo run -p vsm-worker --example harness_in_memory
```

Run the controller/worker/ledger loop:

```bash
cargo run -p vsm-controller --example controller_worker_in_memory
```

Run the System 3* audit smoke test:

```bash
cargo run -p vsm-controller --example controller_audit_smoke
```

Run the bounded mutation trial smoke test:

```bash
cargo run -p vsm-controller --example controller_trial_smoke
```

These commands passed locally on 2026-06-16.

## Codex CLI Harness

Install and authenticate Codex CLI first, then run:

```bash
cargo run -p vsm-worker --example codex_cli_in_memory -- /path/to/workspace
```

The `CodexCliProvider` uses non-interactive Codex execution with stdin:

```text
codex exec --cd <workspace> --sandbox workspace-write --ask-for-approval never --color never --json --ephemeral -
```

For production, run this inside a hardened runner and use stricter sandbox/approval settings where appropriate.

## RabbitMQ-Backed Codex Worker

Start RabbitMQ, ensure Codex CLI is installed/authenticated, then run:

```bash
export VSM_RABBIT_URI="amqp://guest:guest@localhost:5672/%2f"
export VSM_RABBIT_EXCHANGE="vsm.events"
export VSM_PARENT_NODE_ID="root-controller"
export VSM_WORKER_NODE_ID="primary-code-service"
export CODEX_WORKSPACE="/path/to/repo"

cargo run -p vsm-worker --example rabbit_codex_worker
```

In another terminal, publish a smoke-test task:

```bash
export RABBITMQ_URI="amqp://guest:guest@localhost:5672/%2f"
export VSM_RABBITMQ_EXCHANGE="vsm.events"
export VSM_ROOT_NODE_ID="root-controller"
export VSM_WORKER_NODE_ID="primary-code-service"

cargo run -p vsm-worker --example rabbit_publish_task
```

The worker subscribes to `vsm.task_packet` messages on `VsmChannelType::ResourceBargaining` targeted at its node ID and publishes `vsm.task_result` messages back to the source or parent node.

## Design Principles

- Do not predefine a full specialist hierarchy.
- Start with a minimal viable organization.
- Let specialization emerge from observed task clusters and realized outcomes.
- Let System 3 decompose and delegate through VSM channel traffic; derive task graphs from task lineage, dependencies, routing events, handoffs, results, and traces rather than making a separate planning artifact authoritative.
- Use static predicates to guide routing and mutation opportunities, not to prove value.
- Let System 3* suggest genes from audits.
- Evaluate genes through bounded trials.
- Use observed traces for selection.
- Apply parsimony pressure to worker count, prompt size, tool surface, coordination edges, diff size, and organizational complexity.
- Keep code-writing authority restricted to operational leaves.
- Treat the user as part of the environment through directives and algedonic signals, not as the manual gene designer.

## Background Material

- [Viable System Model](https://en.wikipedia.org/wiki/Viable_system_model)
- [Evolutionary Algorithms](https://en.wikipedia.org/wiki/Evolutionary_algorithm)
- [Conway's Law](https://en.wikipedia.org/wiki/Conway%27s_law)
- [Pareto Efficiency](https://en.wikipedia.org/wiki/Pareto_efficiency)
