# How it works

Why things are built the way they are, what was measured rather than assumed,
and what is still rough. The [README](../README.md) covers what the plugin does
and how to use it; this file covers the reasoning.

## Contents

- [Three transports, one API](#three-transports-one-api)
- [Message shapes](#message-shapes)
- [Connecting: the three entry points](#connecting-the-three-entry-points)
- [SWIM mode](#swim-mode)
- [Security model](#security-model)
- [Design notes: measured, not assumed](#design-notes-measured-not-assumed)
- [Testing](#testing)
- [Known limitations](#known-limitations)

## Three transports, one API

`broker`, `mesh` and `swim` all implement the same interface and all pass the
same conformance suite, so their semantics cannot drift apart. Pick one with
`mode`; run one at a time.

| | broker | mesh | swim |
|---|---|---|---|
| Members found by | a broker pushes the roster | nodes exchange member tables directly | SWIM gossip |
| Messages travel | through the broker | direct node-to-node | direct node-to-node |
| Needs | a reachable broker URL | a seed address | a seed + the Go sidecar |
| Extra process per node | none | none | one (the sidecar) |

**Start with `broker`.** It is one process, it survives nodes coming and going,
and it is the only mode where nothing needs to know anyone else's address in
advance.

- **broker** — 2 to 25 nodes, one operator. Everything routes through one
  process; if it is down, nobody talks.
- **mesh** — no centre, but every node must be able to reach every other, and
  each needs at least one known address to start from. Two nodes means one
  link; 25 means 300.
- **swim** — same direct delivery as mesh, but membership and failure detection
  come from SWIM. Use it when nodes actually die without saying goodbye and you
  need the roster to notice.

### Seeds

`mesh` and `swim` have no rendezvous point, so a fresh node knows nobody. A
**seed** is any address of an already-running node; from that one contact it
learns everyone else. You only need one.

`/team status` prints the exact address to hand out.

For `mesh`, the seed is the node's **delivery port**. For `swim`, it is the
sidecar's **gossip port** — a different port. Handing out the wrong one sends
peers to a socket that answers no membership protocol.

A node with no seed still works; it waits for someone to connect to it. `/team
status` says so rather than pretending otherwise.

## Message shapes

```
A → B   request   (no re)
          B runs a turn, its output goes back to A automatically

B → A   reply     (re = A's request id)
          A shows the reply. It does NOT auto-answer, so the exchange ends.

A(model) → B ...    if A wants to continue, its model calls team_send explicitly
```

Rules, enforced by `src/policy.js` and covered by tests:

| Inbound | Action |
|---|---|
| Request (`re` empty) | Inject into the model; auto-reply once |
| Reply to something the **model** sent | Inject, with the original quoted; no auto-reply |
| Reply to something **you** sent via `/team send` | Card only — the model never saw your message, so waking it would confuse it |
| Reply whose original is unknown (e.g. after a restart) | Inject; no auto-reply |
| `fyi` broadcast (`announce=always`) | Card only |

Messages carry a hop count and stop at 4. That is a backstop, not the mechanism —
the shape above is what actually terminates a conversation.

### Concurrent requests

Two peers sending at almost the same moment each get their own answer.

A follow-up opens its own turn, so Pi's event stream looks like this:

```
agent_start
  turn: user "request A" → assistant "answer to A"
  turn: user "request B" → assistant "answer to B"
agent_end → agent_settled
```

Injected user messages come back verbatim on `message_end`, so an answer is
bound to its request by matching that payload. Two requests in one turn share
one answer, which is correct — the model saw both at once. Every reply still
carries its own `re` and `hops`, because that is routing, not text.

Anything that cannot be matched to an answer reports failure to the sender
rather than substituting another request's text. Sending someone an unrelated
answer is worse than telling them it did not work.

## Connecting: the three entry points

The same options work three ways. They share one implementation
(`src/options.js` parses and validates; `src/dispatch.js` decides), so the
command and the tool cannot drift.

**Environment variables** — for scripts, containers, and startup:

```bash
TEAM_NAME=dev01-web \
TEAM_MODE=broker \
TEAM_URL=http://100.64.0.1:8787 \
TEAM_TOKEN=<token> \
TEAM_LABELS=web \
  pi
```

**`/team` commands** — for humans:

```
/team join dev --url http://100.64.0.1:8787 --token <token>
/team mode mesh
/team join dev --mode mesh --seeds 100.64.0.1:19801
```

**`team_*` tools** — for the model and automation: `team_join`, `team_info`,
`team_roster`, `team_send`, `team_label`, `team_leave`.

### What gets saved, and what does not

| Option | Saved to `~/.pi/agent/pi-agent-team/<team>.json` | Why |
|---|---|---|
| `url`, `token`, `mode`, `seeds` | yes | belongs to the team; should not have to be repeated |
| `name`, `labels` | no | belongs to this run |

`name` and `labels` are deliberately not saved. One machine can run several Pi
agents, each an independent node, and they share one team config file. Saving
the node name there means the second agent to start overwrites the first — and
the name *is* the identity.

Changing `mode` keeps `url` and `token`. To move a team from broker to mesh:

```
/team mode mesh
```

## SWIM mode

Membership and failure detection come from [SWIM](https://www.cs.cornell.edu/projects/anticentr/swim/),
implemented by [hashicorp/memberlist](https://github.com/hashicorp/memberlist)
running as a small Go sidecar. Messages do **not** go through the sidecar; they
take the same direct links `mesh` uses. SWIM answers "who is alive", not "carry
this byte".

**Why a sidecar instead of writing SWIM in JavaScript.** SWIM's easy part is
gossip. Its hard parts — incarnation numbers, suspicion timeouts, indirect ping
requests — are the parts that only misbehave when something is already going
wrong. A failure detector that is subtly wrong on a flaky network announces
offline peers that are fine, and then nobody trusts the roster. memberlist is
the implementation Consul and Kubernetes ship.

**Why the sidecar does not carry messages.** If it did, SWIM would become a
message bus, and every message would pay a gossip hop. Keeping it to membership
means the two can be reasoned about separately.

### Build it

```bash
cd swim
go build -o ../.tmp/swim-sidecar .
```

Or point at a build anywhere with `PI_TEAM_SWIM_SIDECAR=/path/to/swim-sidecar`.

Missing sidecar is a hard failure, not a silent fallback to another mode.
Falling back would make SWIM decorative: the member table would not affect how
messages travel, so "SWIM noticed the node died" would be a claim about nothing.

### Two ports, and only one of them is a seed

```
 模式       swim
 连接       online
 seeds      127.0.0.1:7946
 投递端口   19901
 gossip 端口 40269
 种子写法   <本机可达地址>:40269
```

The delivery port travels inside the member table. Only the gossip port is a
join point.

Members advertise their delivery port through memberlist's `Meta` alongside host
and labels. Without that, the member table would know a name and have no way to
reach it.

The gossip key is derived from the team token, so a node with a different token
cannot join the gossip even if it can reach the port.

### Membership and message delivery are separate

In SWIM mode the sidecar owns the member table, so `mesh` must not also run its
own discovery — the two would disagree and dead nodes would linger in the view.
Mesh still exchanges hellos to learn delivery endpoints, but it takes membership
from the sidecar.

## Security model

| Layer | Mechanism |
|---|---|
| Network | Broker binds a single tailnet address — no public listener |
| Transport | WireGuard (Tailscale) encrypts the link; no extra TLS |
| Auth | `TEAM_TOKEN` on every connection, constant-time compared |
| Identity | The connection's name is authoritative; a client's self-declared `from` is overwritten |

**What this does not protect.** The broker sees message plaintext. It does not
persist anything, but it is a single point of trust — only run it on a machine
you control.

| | broker | mesh | swim |
|---|---|---|---|
| Who sees message plaintext | the broker too | only the two endpoints | only the two endpoints |
| Membership decided by | the broker | peers' self-reports | SWIM + the team token |
| Blast radius of a stolen token | can evict any node | can join as a member | can join the gossip |

`mesh` and `swim` remove the broker from the data path. They do not add
authentication beyond the token: a peer that can reach the port and holds the
token is a member.

### Address advertisement

In mesh and swim, a peer's address is learned from **the source address of its
inbound connection**, not from what it claims. A peer cannot make you connect
somewhere it made up.

The consequence: both endpoints must be able to reach each other directly. Two
agents behind NAT with no route between them need the broker.

## Design notes: measured, not assumed

Each of these was a real bug found by testing, not a preference:

- **`sendUserMessage()` is on `ExtensionAPI`, not `ExtensionContext`.**
  `ctx.sendUserMessage()` throws `is not a function`.

- **Display cards use `appendEntry`, not `sendMessage`.**
  `sendMessage({ display: true })` looks like a display-only API, but its
  messages **do enter the LLM context** (verified with the `context` event). The
  symptom was subtle: the model read its own outbound card and concluded a
  teammate had sent it.

- **`agent_settled`, not `agent_end`.** `agent_end` can be followed by retries,
  overflow recovery, compaction and queued continuations. Pushing on
  `agent_end` sends intermediate states.

- **`agent_settled` carries no `messages` field.** Accumulate assistant text on
  `message_end`; the settled event only has `{ type }`.

- **`ctx.isIdle()` is not a reliable "can I inject" test.** It still reports
  idle in the same tick as a previous injection, so a second message took the
  direct path and Pi rejected it with *"Agent is already processing a prompt"* —
  the request vanished. `index.ts` tracks run state from `before_agent_start` to
  `agent_settled` instead.

- **Don't frame every inbound message as an assigned task.** Telling the model
  "this is a task, execute it" fixes passivity but breaks peer conversation — a
  reply gets described as a task. The wording is deliberately neutral: describe
  the source, respond to the content.

- **The broker speaks minimal RFC 6455 by hand.** Only the frames needed; no
  compression, no extensions. `src/ws.test.js` covers the cases that actually
  break in practice — byte-by-byte delivery, fragmentation, the 126/65536 length
  boundaries, and multi-byte UTF-8 split across chunks.

## Testing

```bash
npm test           # everything
npm run test:unit  # frame codec, session, policy, config, mode, options
npm run test:e2e   # all three transports over real sockets
```

The e2e suite starts an isolated broker per test (its own port). Sharing one
broker leaks roster state between tests, and the roster is one of the things
under test.

### One conformance suite, three transports

`e2e/conformance.js` takes a transport factory and runs the same guarantees
against whatever it produces. Broker, mesh and swim all pass it, so their
semantics cannot drift apart. There is no "mesh version" of a guarantee to keep
in sync.

It covers: a sender's claimed identity is ignored in favour of the connection's;
broadcasts never reach their sender; a recipient named twice receives once;
unknown recipients and empty groups are reported rather than silently dropped;
membership converges and excludes self; payloads arrive byte-identical including
`re` and `hops`; `state()` reports lifecycle; delivery is best-effort. There is
also a takeover suite.

**The SWIM tests skip when the sidecar is not built**, so `npm test` works
without Go installed. They are not silently passing — they report as skipped.
Build the sidecar first if you are touching `src/transport-swim.js`:

```bash
cd swim && go build -o ../.tmp/swim-sidecar . && cd .. && npm test
```

## Known limitations

- **Broker restart drops all connections.** Clients reconnect with exponential
  backoff, but messages in flight are lost. Broker is a single point of failure;
  each Pi keeps working normally.
- **Same-name takeover evicts the older connection.** A second node using a live
  node's name takes it over; the older one is closed with code `4001` and stops
  reconnecting (it would otherwise fight the new one forever). Anyone holding
  the token can therefore evict any node. Set `TEAM_NO_TAKEOVER=1` on the broker
  for the old `409 Conflict` behaviour.
- **No offline queue.** A message to an offline node is refused immediately with
  `undeliverable`. Silent queueing makes "did they get it?" unknowable.
- **No delivery receipts beyond "written to the peer's socket."** A successful
  send does not mean the peer's model processed it.
- **The roster is global.** All nodes are in one team; there are no rooms.
- **Labels are a convention.** Nothing checks that `@web` means the same thing
  on every node.
- **The sidecar must be built per platform.** There is no prebuilt binary.
- **SWIM membership is eventually consistent.** A node killed with `SIGKILL`
  stays `alive` for a suspicion timeout before it is marked. Graceful exit is
  immediate.
- **SWIM is beta at 5 nodes.** The conformance suite covers correctness, not
  scale.
