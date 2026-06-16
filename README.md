# vsm-agent-kernel

A Rust scaffold for a recursive Viable System Model (VSM) agent organization whose structure evolves as an organizational genome.

The workspace is split into four crates:

- **`vsm-core`**: pure domain model. Nodes, capabilities, channels, messages, task packets, genome patches, gene suggestions, traces, fitness scaffolding, and the transport trait.
- **`vsm-runtime`**: in-memory transport and mutation-trial utilities for local simulation.
- **`vsm-amqp`**: RabbitMQ/AMQP transport adapter implementing `vsm-core::Transport`.
- **`vsm-worker`**: leaf worker harness. It subscribes to task packets, enforces leaf capabilities, calls a model provider, publishes task results, and records task traces.

The core invariant is enforced by runtime state, not prompt text:

```text
node has children       => metasystem => cannot write code through the worker harness
node has no children    => System 1 leaf => may execute only according to leaf capabilities
```

## Conceptual mapping

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

A parent-child containment edge creates a standard VSM channel bundle: resource bargaining, command, System 2 coordination, System 3* audit, and algedonic upward signaling.


## Core genome mutation

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

That keeps the VSM invariant in code: adding children changes the node's runtime capabilities, independent of what the prompt says.

## Transport model

All communication flows through a broker-agnostic trait:

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn publish(&self, envelope: MessageEnvelope) -> Result<(), TransportError>;
    async fn subscribe(&self, subscription: Subscription) -> Result<EnvelopeStream, TransportError>;
}
```

RabbitMQ is just one adapter. The same worker harness works with in-memory transport, RabbitMQ, NATS, Kafka, Redis streams, HTTP, local IPC, or any adapter implementing this trait.

## Worker harness

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
```

The harness deliberately depends on traits:

```rust
#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse, ModelProviderError>;
}
```

Included model providers:

- `EchoModelProvider`: deterministic local provider for wiring tests.
- `CodexCliProvider`: invokes `codex exec` as a child process.
- `OpenAiCodexProvider`: experimental Responses API provider retained as an adapter example.

## Run the local harness simulation

```bash
cargo run -p vsm-worker --example harness_in_memory
```

This uses `InMemoryTransport` plus `EchoModelProvider`. It should not require RabbitMQ or Codex.

## Run the Codex CLI harness simulation

Install and authenticate the Codex CLI first, then run:

```bash
cargo run -p vsm-worker --example codex_cli_in_memory -- /path/to/workspace
```

The example starts a leaf node, subscribes over the in-memory transport, dispatches one `TaskPacket`, invokes `codex exec`, publishes a `TaskResult` back to the root, and records a `TaskTrace`.

The `CodexCliProvider` uses non-interactive Codex execution with stdin:

```text
codex exec --cd <workspace> --sandbox workspace-write --color never -
```

For production, keep this inside a hardened runner and use stricter sandbox/approval settings where appropriate.

## Run a RabbitMQ-backed Codex worker

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

## Current Status

The key implementation choices are:

- The persistent structure is a tree of viable nodes, not a tree of workers.
- A node with children is a metasystem and cannot write code.
- A leaf node may perform an operation such as coding, review, test, research, or integration.
- Parent-child edges and lateral relations are explicit channel genes.
- Gene suggestions are generated internally by audits, future probes, resource bargaining, coordination signals, and algedonic signals.
- Selection uses observed traces rather than predicted benefit.
- RabbitMQ is only a transport adapter. The core runtime speaks through a Transport trait.

## Background Material
- [Viable System Model](https://en.wikipedia.org/wiki/Viable_system_model)
- [Evolutionary Algorithms](https://en.wikipedia.org/wiki/Evolutionary_algorithm)
- [Conway's Law](https://en.wikipedia.org/wiki/Conway%27s_law)