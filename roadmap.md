# Pi Agent Team roadmap

## 1. Purpose

Pi Agent Team lets several Pi agents work as one team. The agents usually run on
different machines, and one machine may run several of them. They find each
other, send direct messages, and message groups.

The plugin carries messages. It does not know what the messages mean. Concepts
like tasks, runners, repositories, reviews or benchmarks belong to the agents
and to their skills and AGENTS.md files, never to the protocol.

The whole system should stay small enough that one person can read it in an
afternoon, and that an agent can use it without learning distributed-systems
machinery.

## 2. Constraints

These numbers drive the design. When one of them changes, revisit the design.

| Constraint | Value |
|---|---|
| Team size | Up to about 25 agents today. Design target is 32. |
| Agents per machine | Several. Identity is the agent name, never the IP. |
| Operator | One person, who controls every machine. |
| Network | A private network, Tailscale today, with full reachability between machines. |
| Runtime | Node 22 or newer, no npm runtime dependencies. |
| Cost of one delivered message | One full model turn on the recipient. |

The last row matters more than any other. A normal messaging system optimizes
bytes and latency. Here, every delivery wakes a model, spends tokens and takes
seconds. The design therefore avoids waking agents that did not need to wake.

## 3. Design principles

### 3.1 One stable Team API

Applications see Agents, Groups, direct messages and group messages. How those
messages travel is hidden behind a transport. Switching from broker to mesh to
SWIM changes one configuration value and nothing in the skills or the model's
instructions.

### 3.2 No domain semantics in the protocol

This message is valid, and the plugin does not need to understand it:

```text
laptop-web -> dev01-api   "Environment fixed in commit abc123"
```

What "environment" or "commit" means is a convention of the skill that sent it.

### 3.3 The conversation contract belongs to every transport

The rules in section 7 decide when an agent wakes and when it answers. They are
part of the Team API, not of any one transport. A transport that delivers
messages correctly but breaks those rules is broken.

### 3.4 Prefer simple infrastructure

The member list is a convergence problem, not a consensus problem. Each agent
holds a view that may differ for a moment and then settles. Nothing in the core
needs Raft, Paxos, CRDTs, distributed locks or transactions. Add them only for a
concrete requirement, and write that requirement down first.

## 4. Current state

This section describes the code in the repository today.

### What works

The broker transport and the Pi extension run on real machines, across a
tailnet, and 53 tests pass.

- **Broker** (`broker.mjs`). Agents dial out and the broker routes. It binds a
  single address, refuses `0.0.0.0`, and refuses to start without `TEAM_TOKEN`.
  It routes to one name, a list of names, a `#tag` group or everyone (`*`), and
  pushes a roster of `{name, host, addr, tags}`. A new connection with an
  existing name takes the name over. The old connection gets close code `4001`
  and stops reconnecting, and other agents see no leave/join churn.
- **Pi extension** (`index.ts`). It injects peer messages with
  `sendUserMessage()`, so the model handles them as normal input, TUI included.
  It adds the `team_send` tool, the `/team` command with an interactive menu and
  autocompletion, and send/receive/reply/failure cards in the transcript. The
  cards use `appendEntry`, so they never enter the model context.
- **Inbound policy** (`src/policy.js`). A pure module with its own tests. It
  implements the conversation contract in section 7.
- **Skill** (`skills/pi-agent-team/SKILL.md`). It teaches the model the
  difference between a request and a reply.

### Gaps against the target model

| Target | Today |
|---|---|
| Application and control messages travel separately | Both share one envelope. The broker reads `body.kind` to produce `welcome`, `delivered` and similar messages. |
| Opaque `type` and `payload` | Application content sits in `body.text` |
| Explicit Team API and Transport interface in code | The extension talks to the broker socket directly |
| Several transports | Broker only |
| `team_id` | One broker is one team |
| Managed groups | Agents declare their own tags |
| Extension-level tests | Only the policy module is covered. The wiring was tested by hand on live machines. |

## 5. Core model

### 5.1 Agent

```text
Agent {
  name        unique inside the team, [A-Za-z0-9][A-Za-z0-9._-]{0,31}
  host        machine name, for grouping in the UI only
  endpoint    transport-specific address, if the transport needs one
  labels      free-form strings, at most 8
  metadata    application-defined, opaque to the plugin
}
```

The name is the identity. `host` never routes anything. That is also why the
Tailscale peer list cannot be the member table: it only sees machines, and one
machine may host `dev01-web` and `dev01-api` at once.

### 5.2 Team

A team is the set of agents that share a transport and a token. In broker mode
that means one broker. Whether the protocol should also carry an explicit
`team_id` is open, see section 15.

### 5.3 Group

A group is a named subset of agents. The plugin does not interpret group names:
`research`, `runner`, `infra` and `review` are all fine.

Today a group is a label the agent declares for itself, and `#web` means every
agent that declared `web`. This needs no membership registry and raises no
question about who may add whom.

Managed groups, where someone other than the agent changes membership through
`create_group` or `add_group_member`, stay a possible later model. They are
worth building only if an agent must be put into a group by someone else. That
also forces an authorization rule, see section 15.

### 5.4 Capabilities

Capabilities are labels too. `find_agents("docker")` filters members by label.
The plugin gives the label no meaning.

## 6. Messages

### 6.1 Recipients

One field expresses every kind of addressing:

| `to` | Delivered to |
|---|---|
| `"dev01-api"` | One agent |
| `["web1", "db1"]` | Those agents |
| `"#web"` | Every agent carrying label `web`, except the sender |
| `"*"` | Every agent except the sender |
| `["#web", "db1"]` | Union of both, each recipient once |

A split into separate `recipient` and `group_id` fields cannot express the last
row, and the current code already relies on it.

### 6.2 Envelope v1, target

Application message:

```json
{
  "v": 1,
  "id": "m-lz4k2p-a8f3c1",
  "from": "laptop-web",
  "to": ["dev01-api"],
  "re": null,
  "hops": 0,
  "ts": 1790000000000,
  "type": "text",
  "payload": { "text": "Run the integration tests on dev" }
}
```

| Field | Meaning |
|---|---|
| `v` | Envelope version |
| `id` | Unique message id. Applications use it for idempotency. |
| `from` | Sender. The transport sets it from the authenticated connection. A client's own claim never survives. |
| `to` | Recipient expression from 6.1 |
| `re` | Id of the message this one answers. It is what ends an exchange, see section 7. |
| `hops` | Loop guard, incremented on each automatic reply |
| `ts` | Sender clock, milliseconds |
| `type` | Opaque to the transport. The Pi extension defines `text` and `fyi`. |
| `payload` | Opaque to the transport |

Transports carry `re` and `hops` unchanged and never interpret them. The
conversation layer interprets them.

Control messages travel on their own frame shape and never mix with application
traffic:

```json
{ "v": 1, "ctl": "welcome", "self": { "name": "laptop-web" }, "members": [] }
{ "v": 1, "ctl": "member.joined", "agent": { "name": "dev01-api" } }
{ "v": 1, "ctl": "member.left", "agent": { "name": "dev01-api" } }
{ "v": 1, "ctl": "receipt", "re": "m-lz4k2p-a8f3c1",
  "delivered": ["dev01-api"], "failed": [], "unknown": [] }
```

### 6.3 Migrating from the current envelope

The current envelope is `{from, to, id, re, body}`.

| Current | v1 |
|---|---|
| `body.text` | `payload.text` |
| `body.hops` | `hops` |
| `body.fyi: true` | `type: "fyi"` |
| broker messages with `body.kind` | `ctl` frames |
| no version | `v: 1` |
| no timestamp | `ts` |

The client announces its version when it connects. For one release the broker
accepts both, and it logs every v0 client so the operator can see what still
needs upgrading.

## 7. Conversation contract

Every transport must produce this behavior. The current implementation is
`src/policy.js`.

| Inbound message | What the receiving agent does |
|---|---|
| Request, `re` empty | Inject into the model. The turn's final output goes back to the sender once, with `re` set. |
| Reply to a message the model sent | Inject, quoting the original. No automatic answer. |
| Reply to a message a human sent with `/team send` | Show a card only. The model never saw the original, so waking it only confuses it. |
| Reply whose original is unknown, for example after a restart | Inject. No automatic answer. |
| `type: "fyi"` | Show a card only |

A request gets exactly one automatic answer. An agent that wants to continue the
exchange calls `team_send` explicitly. The hop cap of 4 remains as a backstop.
If it ever fires in normal use, treat that as a bug report.

Broadcasting to more than 5 recipients from the UI asks for confirmation,
because each recipient spends a model turn.

## 8. Team API and transport interface

### 8.1 Team API

```text
Team
├── join()          leave()          self()
├── members()       member(name)     find(label)
├── groups()        group_members(label)
├── send(to, type, payload, re?)
└── on_message(handler)   on_membership(handler)
```

The Pi extension is the first consumer. The model reaches the API through the
`team_send` tool, and the human through `/team`.

### 8.2 Transport interface

```text
Transport {
  start(self: Agent)            stop()
  send(envelope) -> Receipt     on_envelope(handler)
  members() -> Agent[]          on_membership(handler)
}

Receipt { delivered: string[], failed: string[], unknown: string[] }
```

### 8.3 What every transport must guarantee

These guarantees are the conformance suite. A transport ships only after it
passes the whole suite.

1. `from` on a delivered message comes from the connection, never from the
   sender's claim.
2. A group or `*` message never reaches its own sender.
3. Each recipient gets a message at most once, even when `to` names it twice.
4. Unknown names and empty groups appear in the receipt. Nothing is dropped
   silently.
5. When a second agent takes a live name, the newest one wins, and the old one
   is told so and stops reconnecting.
6. A join or leave reaches every member's view within a bound that each
   transport documents.
7. Delivery is best effort and at most once. The receipt says a message reached
   the peer, not that the peer's model processed it.
8. `re` and `hops` arrive unchanged.

## 9. Communication modes

The operator selects the mode with one setting, for example `TEAM_MODE=broker`.

### 9.1 Broker, the default

```text
        broker
       /  |   \
      A   B    C
```

Agents dial out to one broker, which routes every message and holds the member
list.

- Easiest to deploy, debug and secure. Only one machine listens on a port.
- The broker is a single point of failure. When it stops, local work continues
  and only cross-machine messages stop.
- The broker process sees message plaintext. The tailnet encrypts each link.

### 9.2 Full mesh

```text
   A ───── B
   │ ╲   ╱ │
   │   ╳   │
   │ ╱   ╲ │
   C ───── D
```

Every agent connects to every other agent directly. A static seed list or the
broker, used only as a rendezvous point, provides the first peers.

- No central relay and no single point of failure for messaging.
- No process besides the sender and recipient sees message plaintext.
- n agents need n(n-1)/2 connections, which is 496 at 32 agents. That is fine
  for this project's size.
- Every agent must listen on a tailnet port. Several agents on one machine need
  distinct ports, so an agent's endpoint is `address:port`.
- The sender resolves `#group` from its own view of the members. Right after a
  join, that view can miss the newcomer for a moment.

### 9.3 SWIM

SWIM provides membership, failure detection and discovery. It does not carry
application messages. The SWIM transport pairs SWIM membership with the same
direct connections the mesh uses for messages.

```text
            SWIM
             │
      membership view
             │
     ┌───────┼───────┐
     ▼       ▼       ▼
   Agent   Agent   Agent
```

- SWIM was built for thousands of nodes with frequent churn. At about 25 agents
  it should not beat the broker or the mesh, and section 12 measures whether it
  does.
- Plain SWIM has no authentication. Anyone who can reach the gossip port can
  claim a name. The team token has to cover the gossip channel too.
- The subtle parts are incarnation numbers and suspicion handling. Get those
  wrong and members flap between alive and dead, or dead members never leave.

### 9.4 Comparison

| | Broker | Full mesh | SWIM |
|---|---|---|---|
| Messaging | Through the broker | Direct | Direct |
| Membership | Broker | Seeds and peer exchange | Gossip |
| Single point of failure | Broker | None | None |
| Ports opened per agent | None | One | One, plus gossip |
| Plaintext visible to | Broker | Sender and recipient only | Sender and recipient only |
| Fits | Default, any size here | Small teams, no central host | Large teams with churn |

## 10. Delivery guarantees

- Best effort and at most once, in every mode.
- No queue for offline agents. The sender gets an immediate `undeliverable`.
  A silent queue would make "did they get it?" unanswerable.
- Nothing is persisted by the transport.
- Applications that need exactly-once behavior deduplicate by message id or by
  their own idempotency key.

Known limitation of the current extension: if two requests arrive while the
model is busy, both get processed but only the last one gets an automatic
answer.

## 11. Security and trust

### Today

- **Network.** Only tailnet members can reach the broker. Tailscale
  authenticates each machine when the connection is set up.
- **Team membership.** A shared token, compared in constant time.
- **Identity.** The broker overwrites `from` with the connection's name.
- **Links.** WireGuard encrypts every link, so there is no extra TLS.

### What this does not protect

- Tailscale authenticates machines, not agent processes. Any process on a tailnet
  machine that holds the token can join under any free name, or take over a live
  one.
- The broker sees plaintext. Nothing is end-to-end encrypted between agents.

### Possible hardening, in order of cost

1. Record the tailnet identity of each connection by asking Tailscale's local API
   who owns the remote address, instead of trusting the self-declared `host`.
   This costs little and turns `host` into a verified value.
2. Restrict takeover to connections from the same tailnet machine as the current
   holder of the name.
3. Per-agent keys, signed messages and group-level authorization. These are only
   worth it once managed groups exist.

## 12. Roadmap

Each phase lists its exit criteria. A phase is done when its criteria hold, not
when its code exists.

### Phase 0, broker MVP: done

Delivered: broker transport, Pi extension, inbound policy, skill, same-name
takeover, multicast with labels, `/team` menu, transcript cards.

Evidence: 53 tests pass, covering frame codec, broker end-to-end and inbound
policy. A live test ran three agents on two machines, with two agents on one
machine.

### Phase 1, envelope v1 and the Team API in code

- Implement the v1 envelope and separate `ctl` frames, per section 6.
- Introduce the Team API and the transport interface. Refactor the broker path
  into a `BrokerTransport` without changing behavior.
- Add extension-level tests: broker restart mid-conversation, peer going offline
  between a request and its reply, two requests arriving in one turn.

Exit criteria: the broker never reads `payload`; all existing tests pass
unchanged; v0 clients still work for one release and appear in the broker log.

### Phase 2, conformance suite

Turn section 8.3 into one test suite that takes a transport factory as input.

Exit criteria: `BrokerTransport` passes; each guarantee has at least one test
that fails when the guarantee is broken on purpose.

### Phase 3, skill and AGENTS.md integration

Show how applications build their own coordination on top of Team, without adding
anything to the protocol. Provide example skills for collaborative coding,
multi-agent review and infrastructure repair, plus an AGENTS.md snippet.

Exit criteria: each example runs on two machines and needs no plugin change.

### Phase 4, full mesh

Implement `MeshTransport`. Bootstrap from a static seed list, with the broker as
an optional rendezvous.

Exit criteria: passes the conformance suite. Tested with at least 8 agents,
including several on one machine. Messaging keeps working with the broker
stopped.

### Phase 5, SWIM

Implement `SwimTransport`: SWIM for membership, direct connections for messages.
Decide first between a hand-written implementation and a sidecar built on an
existing library such as memberlist.

**Done — sidecar chosen, implemented, conformance passed.**

`swim/` is a Go sidecar on `hashicorp/memberlist`. Go because SWIM's hard parts
(incarnation numbers, suspicion timeouts, indirect ping requests) only misbehave
when something is already wrong, and a failure detector that is subtly wrong
causes the roster to be distrusted. memberlist is what Consul and Kubernetes
ship. `SwimTransport` takes membership from the sidecar and reuses mesh's
connection layer for delivery, so delivery cannot diverge between the two modes.

Passes the same conformance suite as broker and mesh. Verified live across three
TUI nodes, including discovery, labels, graceful leave, SIGKILL detection via
suspicion timeout, and a node with a different token being invisible (the gossip
key derives from the team token).

Two things the work surfaced:

- Members must advertise their *delivery* port through memberlist's Meta.
  SWIM's own announcement carries the gossip port; without the extra field the
  member table knows a name and has no way to reach it.
- Mesh needed an `externalMembership` mode. If it kept running its own discovery
  alongside the sidecar, the two disagreed and dead nodes lingered in the view.

**Not done — the benchmark.** Still to run: lightweight simulated nodes (not
real Pi sessions) at 25 / 100 / 500 / 1,000, measuring membership convergence,
failure detection latency, bandwidth, CPU and memory, against broker and mesh at
the sizes where all three run.

Exit criteria: conformance suite passes — **met**. The benchmark report says at
what team size, if any, SWIM beats the other modes, and the default mode follows
that result — **not met**; `broker` remains the default, which is the honest
choice while the data is missing rather than a claim that SWIM is worse.

### Phase 6, group-aware large clusters

Consider only if phase 5 data shows a need. Candidates are group-local
membership, cross-group discovery, group gateways and hierarchical routing.

## 13. Persistence and application state

Persistence is outside the Team abstraction. The transport stores nothing.

Skills that need shared state can run their own store, for example Redis or
Valkey for presence, queues or application data. The plugin never requires one.

## 14. Lessons from building phase 0

Each of these was a real failure.

**Politeness produces nothing.** An injected message framed only as "a message
from another Pi node" got answers like "Ready for the task". The model needs to
be told what to do with the message. The wording took three attempts.

**Over-correcting breaks conversation.** Framing every inbound message as "a task
assigned to you, execute it" fixed the passivity. It also turned a teammate's
next line in a shared poem into a "task".

**Replies must not be answered automatically.** When automatic answers applied to
replies too, two agents ping-ponged until the hop cap, five round trips in the
broker log. Section 7 is the fix.

**A card is not a message.** `sendMessage({display: true})` looks display-only,
but its messages enter the model context. The model read its own outbound card,
concluded that a teammate had sent it, and stopped collaborating. `appendEntry`
is the API documented as not sent to the model.

**Asking the model is not a test.** A model asked whether a marker was in its
context said no while the marker was there. The extension `context` event is the
only reliable check.

**The two extension objects differ.** `sendUserMessage` lives on `ExtensionAPI`
and `isIdle` on `ExtensionContext`. Mixing them up fails at runtime, not at load.

**A human's message is invisible to the model.** Text sent with `/team send`
never enters the model context. A reply to it must not wake the model, which
otherwise answers "your message had an empty body".

## 15. Open questions

**`team_id` on the wire.** Needed only if one broker should serve several teams.
Recommendation: leave it out of v1. Add a `team` field in a later version if the
need appears.

**Managed groups.** Only needed if someone other than the agent must change group
membership. That requires an authorization rule for who may add whom.
Recommendation: keep self-declared labels until a concrete case appears.

**Several pending automatic answers.** The extension keeps one pending answer
slot, so one of two concurrent requests goes unanswered. A small queue fixes it.
The open part is how answers map to requests when the model handles both in one
turn.

**Offline delivery.** Queueing for offline agents contradicts the current
guarantee. If it is ever needed, it belongs in an optional store, not in the
transport.

**End-to-end encryption.** The mesh and SWIM modes already keep plaintext away
from any relay. For broker mode it would need per-agent keys, which ties into the
hardening list in section 11.

## 16. Non-goals

The plugin is not:

- a task scheduler, workflow engine or job queue
- an agent orchestrator. No agent commands another at the protocol level.
- a distributed database or a consensus system
- a Git manager, a Harbor controller or a benchmark framework

Applications adopt no particular workflow to use it.
