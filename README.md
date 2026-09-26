# pi-agent-team

Turn several machines running the [Pi coding agent](https://github.com/earendil-works/pi)
into one team. Each node keeps its own filesystem, session and context; they
discover each other and exchange messages over your tailnet.

No public relay, no phone app, no account. One small broker you own, plus a Pi
extension.

```
  laptop                          dev01
 ┌──────────────┐              ┌──────────────┐
 │ pi (TUI)     │              │ pi (TUI)     │
 │  + index.ts  │              │  + index.ts  │
 └──────┬───────┘              └──────┬───────┘
        │  ws://<tailnet>:8787        │
        └──────────► broker ◄─────────┘
                  (your machine)
```

## What it does

- **Discovery** — every node dials out to the broker, which pushes the roster.
  Members carry `host` and `tags`, so the TUI groups them by machine.
- **Several agents per machine** — identity is the node name, not the IP. One
  server can run `dev01-web` and `dev01-api` side by side. This is why the
  Tailscale peer list cannot serve as the member table: it only sees machines.
- **Unicast, multicast, groups** — send to one node, a list (`a,b`), a tag group
  (`#web`), or everyone (`*`).
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
  Arguments autocomplete node names, `#groups` and subcommands.
- **Agent skill included** — the model reads `skills/pi-agent-team/SKILL.md`.

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
/team send "#web" deploy is starting
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
| `/team status` | Node name, tags, connection, broker URL, peer count, announce mode |
| `/team peers` | Online nodes grouped by machine, plus available `#groups` |
| `/team send <to> <text>` | Send. `to` = name, `a,b`, `#tag`, or `*` |
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
npm test           # unit + e2e
npm run test:unit  # frame codec only
npm run test:e2e   # broker over real WebSockets
```

The e2e suite starts an isolated broker per test (its own port). Sharing one
broker leaks roster state between tests — and the roster is one of the things
under test.

## Known limitations

- **Broker restart drops all connections.** Clients reconnect with exponential
  backoff, but messages in flight are lost.
- **Same-name takeover evicts the older connection.** A second node using a live node's name takes it over; the older one is closed with code `4001` and stops reconnecting (it would otherwise fight the new one forever). Anyone holding the token can therefore evict any node. The token is the only credential and it never leaves your tailnet, so that is accepted. Set `TEAM_NO_TAKEOVER=1` on the broker for the old `409 Conflict` behaviour.
- **No message persistence, no delivery receipts beyond "written to the peer's socket."** A successful send does not mean the peer's model processed it.
- **One pending auto-reply per node.** If two requests arrive while the model is busy, both are processed, but only one auto-reply goes out — to whichever arrived last. The other sender gets no automatic answer.
- **The roster is global.** All nodes are in one team; there are no rooms.
- **Tags are a convention.** Nothing checks that `#web` means the same thing on every node.

## Contributing

Issues and PRs welcome. If you touch the message framing, run `npm test` — the
byte-level cases in `src/ws.test.js` exist because they failed once.

## License

MIT
