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

- **Discovery** — every node dials out to the broker; `list_peers` is just the
  roster the broker pushes.
- **Two-way messaging** — the model calls the `team_send` tool, or you type
  `/team say <node> <text>`.
- **Inbound messages become real turns** — a teammate's message is injected with
  `sendUserMessage()`, so the model processes it exactly as if you had typed it.
  Works in the TUI.
- **Visible in the transcript** — `📥 RECV` / `📤 SEND` / `🔁 REPLY` / `⚠️ FAIL`
  cards, with peer name and timestamp. Cards are display-only and never enter
  the model's context (see [Design notes](#design-notes)).
- **Agent skill included** — the model reads `skills/pi-agent-team/SKILL.md` and
  knows how to behave in a team.

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
TEAM_NAME=laptop \
TEAM_TOKEN=<the token> \
  pi --extension /path/to/pi-agent-team/index.ts
```

Or install as a Pi package:

```bash
pi install git:github.com/Yiki21/pi-agent-team
```

### 3. Verify

```
/team status     # node name, connection, broker, peers, announce mode
/team peers      # who is online
/team say other  hello from laptop
```

The other side sees the message arrive as a `📥 RECV` card, and its next turn's
output is sent back to you automatically.

## Commands

| Command | What it does |
|---|---|
| `/team status` | Node name, connection state, broker URL, peers, announce mode |
| `/team peers` | List online nodes |
| `/team say <name\|all> <text>` | Send manually |
| `/team announce <off\|auto\|always>` | Set the auto-push mode |
| `/team on` / `/team off` | Shorthand for `announce auto` / `announce off` |

### Auto-push modes

| Mode | Behaviour |
|---|---|
| `off` | Only sends on `/team say` or an explicit `team_send` |
| `auto` *(default)* | Also replies to the peer whose message triggered the current turn |
| `always` | Pushes every turn's output to all online nodes |

`always` on two nodes makes them talk to each other indefinitely, each burning
its own tokens. That is why it is not the default.

## How a message flows

```
A's model calls team_send({ to: "B", text: "..." })
   → broker routes to B
   → B injects "[来自 A 的 team 消息] ..." via sendUserMessage()
   → B's model runs a normal turn
   → B's output is sent back to A (announce=auto)
   → A injects it the same way
```

Messages carry a hop count. At 4 hops the chain stops, so an echo between two
`always` nodes cannot run forever.

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
- **Name collision.** A second node using a live node's name is rejected with
  `409`. The old connection keeps the name until its heartbeat expires (default
  30 s). Kicking the old connection instead would be friendlier, but it also
  means anyone holding the token can evict any node — an open decision.
- **No message persistence, no delivery receipts beyond "written to the peer's
  socket."** A successful send does not mean the peer's model processed it.
- **The roster is global.** All nodes are in one team; there are no rooms.

## Contributing

Issues and PRs welcome. If you touch the message framing, run `npm test` — the
byte-level cases in `src/ws.test.js` exist because they failed once.

## License

MIT
