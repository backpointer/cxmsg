# Domain language

This file defines the project terms used in architecture and improvement
documents. Code and documentation should preserve these distinctions because
they carry different authority and delivery semantics.

## Messaging

- **Peer Message**: untrusted coordination text delivered to a Codex or Claude
  peer. It carries information, never user authority.
- **Steering**: adding a Peer Message to a specific in-progress Codex turn.
  Steering does not create a new turn or expand that turn's permissions.
- **Scheduled Peer Message**: a durable Peer Message held until an explicit
  Trigger becomes eligible and the target can accept it.
- **Trigger**: the condition that makes a Scheduled Peer Message eligible.
  Initial trigger kinds are `when-idle`, `after-turn`, and `after-job`.
- **Delivery**: a transport attempt. Transport acceptance is not equivalent to
  model completion.

## Work and authority

- **Delegation**: a user-authorized, correlated job bounded by a stored grant,
  permission profile, execution mode, and approval policy.
- **Job**: a durable correlation record for Delegation or cross-runtime
  delivery. A Job has one stable ID and an explicit terminal state.
- **Busy**: a Codex thread has an in-progress turn. A bridge or worker process
  being alive does not by itself make the thread Busy.
- **Idle**: a Codex thread has no in-progress turn and can accept direct input.

## Lifecycle and diagnosis

- **Turn Lifecycle Module**: the Module that observes App Server turn state,
  reconciles missed events, and exposes a stable Busy/Idle view.
- **Scheduler Module**: the Module that durably stores Triggered work, claims it
  once, and dispatches it under per-target ordering rules.
- **Doctor Module**: the read-only diagnostic Module that collects evidence
  through existing runtime Interfaces and reports health without changing
  local state or starting model work.
- **Repair**: an explicit, separately authorized mutation associated with one
  Doctor finding. Repair is not part of the default Doctor Interface.
- **Reconciliation**: comparing durable records with current App Server,
  process, socket, and worker evidence after a restart or missed event.
