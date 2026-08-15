# Identity Lifecycle v1

## Scope

This contract fixes explicit Project and Node lifecycle behavior before Direct
Conversation is introduced. Identity transitions are local coordination
metadata. They never grant permission, migrate authority, move Deliveries, or
approve work.

## Project transitions

Project ID is stable and private. Routing ID, filesystem paths, Git remote
URLs, directory names, and discovery evidence are not Project identity.

### Worktree alias

Git worktrees whose canonical Git common directory is identical belong to the
same Project. Observing another worktree appends or refreshes a root alias; it
does not create a transition or change Project ID. Equal basenames, remote
URLs, or labels are insufficient.

### Move

`cxmsg directory project move <project> <new-root>` is the only v1 operation
that changes a Project discovery key. It:

1. resolves one existing Project by stable ID or routing ID;
2. rejects a destination already owned by another Project;
3. writes an owner-private append-only `move` transition before changing the
   Project head;
4. preserves prior root aliases and adds the new canonical root;
5. keeps every Node's Project ID unchanged.

A repeated move to the current discovery identity is a no-op and creates no
new transition. Doctor verifies that transitions form one unbranched,
acyclic chain ending at the current Project head. A durable transition whose
head update was interrupted is reported as recoverable; repeating the exact
move completes it. Doctor never performs the move.

### Merge and split

Project merge and split are intentionally unsupported in v1. Neither path,
name, Git remote, successor relation, nor Cluster membership may infer them.
A future implementation requires a separate transaction that freezes an exact
Node set, rewrites explicit membership references, preserves source Project
Tombstones, and emits an audit receipt. Until then, operators create distinct
Projects and migrate nothing automatically.

## Node successors

The stable Node key remains `(runtime kind, native ID)`. A restarted Claude
conversation with a new session ID is a new Claude Node. The generic explicit
successor command supports Codex-to-Codex and Claude-to-Claude relations when
both identities belong to the same Project.

A successor edge records continuity context only. It does not inherit or
transfer:

- route role or binding;
- grant, permission profile, approval state, or capability;
- Conversation or Cluster membership;
- Delivery, Job, reply, or correlation ownership;
- Endpoint selection or aliases.

Cross-runtime successor edges remain structurally possible only when explicitly
requested, but they create no runtime adapter or authority. Doctor reports
missing references, Project mismatch, multiple predecessors, and cycles without
repair.

## Scheduled predecessor policy

A Scheduled Peer Message remains pinned to its original Codex thread. Once an
explicit successor edge names that thread as a predecessor, the Scheduler
blocks the Delivery with `ETARGETPREDECESSOR` before target access. It checks
again after claim acquisition; a newly linked successor releases the unused
claim with zero attempts.

The Scheduler never follows the edge or changes the target. The operator must
cancel the old schedule and enqueue a new Logical Message for the intended
successor. The new send receives a new Logical Message ID and passes ordinary
Project, role, expiry, grant, and permission checks independently. Doctor
reports retained predecessor schedules and performs no cancellation or
migration.

Malformed successor storage blocks scheduled dispatch with
`ESUCCESSORUNAVAILABLE`; it is not treated as evidence that no successor
exists.
