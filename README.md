# vsm-agent-kernel

A Rust scaffold for a recursive Viable System Model (VSM) agent organization whose structure evolves as an organizational genome.

The key implementation choices are:

- The persistent structure is a tree of **viable nodes**, not a tree of workers.
- A node with children is a metasystem and **cannot write code**.
- A leaf node may perform an operation such as coding, review, test, research, or integration.
- Parent-child edges and lateral relations are explicit channel genes.
- Gene suggestions are generated internally by audits, future probes, resource bargaining, coordination signals, and algedonic signals.
- Selection uses observed traces rather than predicted benefit.
- RabbitMQ is only a transport adapter. The core runtime speaks through a `Transport` trait.

## Layout

```text
src/
  audit.rs            System 3* audit outputs and gene suggestions
  channel.rs          VSM channel and relation genes
  error.rs            Kernel error type
  genome.rs           Organizational genome and patch application
  ids.rs              Newtype IDs
  lib.rs              Public module exports
  message.rs          Transport envelope and payloads
  node.rs             Recursive viable node model and capability rules
  runtime.rs          Minimal runtime shell
  selection.rs        Observed-fitness scoring helpers
  task.rs             Directive/task packet model
  trace.rs            Task trace ledger inputs
  transport/
    mod.rs            Transport trait
    in_memory.rs      Local test transport
    rabbitmq.rs       Optional RabbitMQ adapter, feature-gated
examples/
  local_sim.rs        Minimal local genome mutation simulation
```

## Run locally

```bash
cargo run --example local_sim
```

## RabbitMQ feature

```bash
cargo build --features rabbitmq
```

The RabbitMQ adapter is intentionally small and platform-agnostic. External workers can be implemented in Rust, Python, containers, model-serving runtimes, or any other process that exchanges JSON envelopes over the broker.

## Conceptual mapping

```text
ViableNode
├── System 5: identity, policy, non-negotiable constraints
├── System 4: future probes and adaptation
├── System 3: resources, decomposition, command, integration
├── System 3*: audit channel and gene suggestion
├── System 2: coordination between children
└── System 1:
    ├── child viable nodes, if internal/metasystem
    └── leaf operation, if no children
```

The runtime enforces:

```text
children.len() > 0 => can_write_code == false
children.len() == 0 && operation == Coding => can_write_code == true
```

Promoting a leaf into a metasystem moves the former operation into a child and clears operational permissions from the parent.
