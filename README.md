# pi-agent-team

Turn several machines running the [Pi coding agent](https://github.com/earendil-works/pi)
into one team. Each node keeps its own filesystem, session and context; they
discover each other and exchange messages over your tailnet.

No public relay, no phone app, no account.

## Three ways to connect

All three share one Team API and pass one conformance suite, so the semantics
do not drift between them. Pick one with `TEAM_MODE`; run one at a time.

| mode | how members are found | how messages travel | needs |
|---|---|---|---|
| `broker` **(default)** | a broker you run pushes the roster | through the broker | a reachable broker URL |
| `mesh` | nodes exchange member tables directly | direct node-to-node links | one seed address |
| `swim` | SWIM gossip (Go sidecar, memberlist) | direct node-to-node links | a seed + the sidecar built |

**Start with `broker`.** It is one process, it survives nodes coming and going,
and it is the only mode where nothing needs to know anyone else's address in
advance.

```
 broker mode                       mesh / swim mode
 ┌────────┐  ┌────────┐            ┌────────┐  ┌────────┐
 │ pi     │  │ pi     │            │ pi     │──│ pi     │
 └───┬────┘  └───┬────┘            └───┬────┘  └───┬────┘
     │ ws    ws │                     │  direct   │
     └────┬─────┘                     └─────┬─────┘
       broker                          (no centre)
```

### Which mode, honestly

- **broker** — 2 to 25 nodes, one operator. Everything routes through one
  process; if it is down, nobody talks. Simplest thing that works.
- **mesh** — no centre, but every node must be able to reach every other, and
  each needs at least one known address to start from. Two nodes means one
  link; 25 means 300.
- **swim** — same direct delivery as mesh, but membership and failure detection
  come from SWIM. Use it when nodes actually die without saying goodbye and you
  need the roster to notice. See [SWIM mode](#swim-mode).

### Seeds, and why you need one

`mesh` and `swim` have no rendezvous point, so a fresh node knows nobody. A
**seed** is any address of an already-running node; from that one contact it
learns everyone else. You only need one.

`/team status` prints the exact address to hand out:

```
 模式       mesh
 连接       online
 seeds      (无,只能被动等待别人连你)
 投递端口   19801
 种子写法   <本机可达地址>:19801
```

A node with no seed still works — it just waits for someone to connect to it.
The status line says so rather than pretending otherwise.

## What it does

- **Discovery** — every node dials out to the broker, which pushes the roster.
  Members carry `host` and `labels`, so the TUI groups them by machine.
- **Several agents per machine** — identity is the node name, not the IP. One
  server can run `dev01-web` and `dev01-api` side by side. This is why the
  Tailscale peer list cannot serve as the member table: it only sees machines.
- **Unicast, multicast, labels** — send to one node, a list (`a,b`), a label
  group (`@web`), or everyone (`*`).
- **Inbound messages become real turns** — a teammate's request is injected with
  `sendUserMessage()`, so the model processes it as if you had typed it. Works
  in the TUI.
- **Sane conversation shape** — a request gets one automatic reply, then stops.
  Replies carry `re` and are never auto-answered. See
  [How a message flows](#how-a-message-flows).
- **Visible in the transcript** — `📥 RECV` / `📤 SEND` / `🔁 REPLY` / `⚠️ FAIL`
  cards, with peer and timestamp. Display-only; they never enter the model's
  context.
- **`/team` menu** — run `/team` with no arguments for an interactive menu.
  Arguments autocomplete node names, `@labels` and subcommands.
- **Agent skill included** — the model reads `skills/pi-agent-team/SKILL.md`.

## SWIM mode

Membership and failure detection come from [SWIM](https://www.cs.cornell.edu/projects/anticentr/swim/),
implemented by [hashicorp/memberlist](https://github.com/hashicorp/memberlist)
running as a small Go sidecar. Messages do **not** go through the sidecar: they
take the same direct links `mesh` uses. SWIM answers "who is alive", not "carry
this byte".

**Why a sidecar instead of writing SWIM in JavaScript.** SWIM's easy part is
gossip. Its hard parts — incarnation numbers, suspicion timeouts, indirect
ping requests — are the parts that only misbehave when something is already
going wrong. A failure detector that is subtly wrong on a flaky network
announces offline peers that are fine, and then nobody trusts the roster.
memberlist is the implementation Consul and Kubernetes ship.

### Build it

```bash
cd swim
go build -o ../.tmp/swim-sidecar .
```

Or point at a build anywhere with `PI_TEAM_SWIM_SIDECAR=/path/to/swim-sidecar`.

Missing sidecar is a hard failure, not a silent fallback to another mode.
Falling back would make SWIM decorative: the member table would not affect how
messages travel, so "SWIM noticed the node died" would be a claim about nothing.

### Config

```bash
TEAM_MODE=swim
TEAM_NAME=dev01-api
TEAM_SEEDS=100.99.85.111:7946      # any running node's GOSSIP port
TEAM_TOKEN=<same token as everyone>
TEAM_LISTEN_PORT=19801             # direct-message port
TEAM_ADVERTISE_HOST=<address others reach you at>
```

The gossip key is derived from the team token, so a node with a different token
cannot join the gossip even if it can reach the port.

### Status: two ports, and only one of them is a seed

```
 模式       swim
 连接       online
 seeds      127.0.0.1:7946
 投递端口   19901
 gossip 端口 40269
 种子写法   <本机可达地址>:40269
```

**Hand out the gossip port as a seed, not the delivery port.** They are
different ports and only the gossip one is a join point; the delivery port
travels inside the member table.

### Limits worth knowing

- The sidecar must be built per platform. There is no prebuilt binary.
- Membership is eventually consistent: a node killed with `SIGKILL` stays
  `alive` for a suspicion timeout before it is marked. Graceful exit is
  immediate.
- Beta at 5 nodes. The conformance suite covers correctness, not scale.

## Requirements

- Node 22+ (uses the built-in `WebSocket` and `crypto.randomUUID`)
- Pi
- [Tailscale](https://tailscale.com/) (or any private network — the broker just
  needs an interface only your machines can reach)

Zero runtime dependencies. No `npm install` step for the extension itself.

## Quick start

### 1. Broker (one machine, ideally an always-on server)

```bash
git clone https://github.com/Yiki21/pi-agent-team
cd pi-agent-team

export TEAM_TOKEN="$(openssl rand -hex 32)"     # save this
node broker.mjs --bind "$(tailscale ip -4)" --port 8787
```

The broker **refuses to start** without `TEAM_TOKEN`, and refuses to bind
`0.0.0.0`. Bind to your tailnet address.

To keep it running:

```bash
sudo tee /etc/systemd/system/pi-agent-team-broker.service >/dev/null <<EOF
[Unit]
Description=pi-agent-team broker (tailnet only)
After=network-online.target tailscaled.service

[Service]
ExecStart=/usr/bin/env TEAM_TOKEN=REPLACE_ME node /path/to/broker.mjs --bind YOUR_TAILSCALE_IP
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl enable --now pi-agent-team-broker
```

Health check: `curl http://<tailnet-ip>:8787/health`

### 2. Each Pi

```bash
TEAM_URL=ws://<tailnet-ip>:8787 \
TEAM_NAME=laptop-web \
TEAM_TAGS=web,frontend \
TEAM_TOKEN=<the token> \
  pi --extension /path/to/pi-agent-team/index.ts
```

`TEAM_NAME` must be unique across the team. `TEAM_TAGS` is optional and used for
group sends. Multiple PIs on one machine just need different names.

Or install as a Pi package:

```bash
pi install git:github.com/Yiki21/pi-agent-team
```

### 3. Verify

```
/team            # interactive menu
/team peers      # who is online, grouped by machine
/team send other hello from laptop
/team send "@web" deploy is starting
/team send '*' maintenance in 5 minutes
```

The other side sees a `📥 RECV` card, and its next turn's output is sent back to
you automatically.

## Commands

Run `/team` with no arguments for a menu; the subcommands below are for when you
already know what you want.

| Command | What it does |
|---|---|
| `/team` | Interactive menu: members, send, broadcast, announce mode, status |
| `/team status` | Node name, labels, mode, connection, seed/port info, peer count, announce mode |
| `/team peers` | Online nodes grouped by machine, plus available `@labels` |
| `/team send <to> <text>` | Send. `to` = name, `a,b`, `@label`, or `*` |
| `/team announce <off\|auto\|always>` | Set the auto-push mode |
| `/team on` / `/team off` | Shorthand for `announce auto` / `announce off` |

Sending to more than 5 recipients asks for confirmation first — each recipient
costs a full model turn.

### Auto-push modes

| Mode | Behaviour |
|---|---|
| `off` | Only sends on an explicit `/team send` or `team_send` |
| `auto` *(default)* | Also replies once to the peer whose request triggered this turn |
| `always` | Mirrors every turn's output to all online nodes, as `fyi` |

`always` marks its messages `fyi`, so recipients show a card without waking their
model. Without that, N nodes on `always` would each trigger N-1 extra turns per
round.

## How a message flows

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
| Reply to something **you** sent via `/team send` | Card only — the model never saw your message, so waking it would just confuse it |
| Reply whose original is unknown (e.g. after a restart) | Inject; no auto-reply |
| `fyi` broadcast (`announce=always`) | Card only |

Messages carry a hop count and stop at 4. That is a backstop, not the mechanism —
the shape above is what actually terminates a conversation.

## Security model

| Layer | Mechanism |
|---|---|
| Network | Broker binds a single tailnet address — no public listener |
| Transport | WireGuard (Tailscale) encrypts the link; no extra TLS |
| Auth | `TEAM_TOKEN` on every connection, constant-time compared |
| Identity | The connection's name is authoritative; a client's self-declared `from` is overwritten |

**What this does not protect.** The broker sees message plaintext. It does not
persist anything, but it is a single point of trust — only run it on a machine
you control. There is no end-to-end encryption between nodes.

**Broker is a single point of failure.** If it goes down, cross-machine messages
stop. Each Pi keeps working normally.

**No offline queue.** A message to an offline node is refused immediately with
`undeliverable`, rather than queued. Silent queueing makes "did they get it?"
unknowable.

**What changes per mode.**

| | broker | mesh | swim |
|---|---|---|---|
| Who sees message plaintext | the broker too | only the two endpoints | only the two endpoints |
| Membership is decided by | the broker | peers' self-reports | SWIM + the team token |
| Node's listening port | not needed | must be reachable by every peer | same as mesh |
| Blast radius of a stolen token | can evict any node | can join as a member | can join the gossip |

`mesh` and `swim` remove the broker from the data path, so it never sees message
content. They do not add authentication beyond the token: a peer that can reach
the port and holds the token is a member. In `swim` the token doubles as the
gossip key, so a wrong token cannot even complete the handshake.

## Design notes

Things that were measured rather than assumed — each of these was a real bug
found by testing:

- **`sendUserMessage()` is on `ExtensionAPI`, not `ExtensionContext`.**
  `ctx.sendUserMessage()` throws `is not a function`.

- **Display cards use `appendEntry`, not `sendMessage`.**
  `sendMessage({ display: true })` looks like a display-only API, but its
  messages **do enter the LLM context** (verified with the `context` event). The
  symptom was subtle: the model read its own outbound card and concluded a
  teammate had sent it. `appendEntry` + `registerEntryRenderer` is documented as
  "not sent to LLM" and stays out.

- **`agent_settled`, not `agent_end`.** `agent_end` can be followed by retries,
  overflow recovery, compaction and queued continuations. Pushing on
  `agent_end` sends intermediate states.

- **`agent_settled` carries no `messages` field.** Accumulate assistant text on
  `message_end`; the settled event only has `{ type }`.

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
npm run test:unit  # frame codec, session, policy, config, mode selection
npm run test:e2e   # all three transports over real sockets
```

The e2e suite starts an isolated broker per test (its own port). Sharing one
broker leaks roster state between tests, and the roster is one of the things
under test.

### One conformance suite, three transports

`e2e/conformance.js` takes a transport factory and runs the same guarantees
against whatever it produces. Broker, mesh and swim all pass it, so their
semantics cannot drift apart. There is no "mesh version" of a guarantee to
keep in sync.

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
  backoff, but messages in flight are lost.
- **Same-name takeover evicts the older connection.** A second node using a live node's name takes it over; the older one is closed with code `4001` and stops reconnecting (it would otherwise fight the new one forever). Anyone holding the token can therefore evict any node. The token is the only credential and it never leaves your tailnet, so that is accepted. Set `TEAM_NO_TAKEOVER=1` on the broker for the old `409 Conflict` behaviour.
- **No message persistence, no delivery receipts beyond "written to the peer's socket."** A successful send does not mean the peer's model processed it.
- **One pending auto-reply per node.** If two requests arrive while the model is busy, both are processed, but only one auto-reply goes out — to whichever arrived last. The other sender gets no automatic answer.
- **The roster is global.** All nodes are in one team; there are no rooms.
- **Labels are a convention.** Nothing checks that `@web` means the same thing on every node.

## Contributing

Issues and PRs welcome. If you touch the message framing, run `npm test` — the
byte-level cases in `src/ws.test.js` exist because they failed once.

## License

MIT
