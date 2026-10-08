# pi-agent-team

Turn several machines running the [Pi coding agent](https://github.com/earendil-works/pi)
into one team. Each node keeps its own filesystem, session and context; they
discover each other and exchange messages over your tailnet.

No public relay, no phone app, no account.

```
  laptop                                    dev01
 ┌──────────────┐                        ┌──────────────┐
 │ pi  dev01-web│◄──── messages ────────►│ pi  srv-api  │
 └──────────────┘                        └──────────────┘
```

## What it does

- **Nodes find each other.** Every node appears in a roster with its machine and
  labels.
- **Send to one, several, a label group, or everyone.** `other`, `a,b`, `@web`,
  `*`.
- **A teammate's message reaches the model without interrupting it.** It is
  delivered as a custom message: while the agent is mid-tool-call it queues, and
  the model sees it at the next turn instead of having its work cut short.
- **The model answers with `team_send`.** No output is mirrored automatically,
  so a run that was doing something else cannot have its tail sent to the wrong
  peer as a "reply".
- **The conversation ends.** A request is answered; a reply is not. Two agents
  cannot talk in circles.
- **You can see it.** `📥 RECV` / `📤 SEND` / `🔁 REPLY` / `⚠️ FAIL` cards in the
  transcript. Display only — they never enter the model's context.
- **Two entry points.** `/team` commands for you, `team_*` tools for the model.
  Both share one implementation, so they cannot behave differently.
- **An agent skill ships with it**, so the model knows the commands without
  being told.

## pi-agent-team or A2A?

[`@bacnh85/pi-a2a`](https://pi.dev/packages/@bacnh85/pi-a2a) also connects Pi
to other agents, over the [A2A protocol](https://a2a-protocol.org/). Neither
package replaces the other. The difference that decides it is where an incoming
message runs.

pi-agent-team delivers a teammate's message into the Pi session that is already
running. The model that reads it has that session's context: the task in
progress, what it already tried, what it has changed. It answers with
`team_send` when it chooses to.

pi-a2a starts a new, isolated Pi session for each incoming task, runs it to
completion and returns the reply as a task artifact. That session works in the
same repository but starts with an empty context. Anything the job needs has to
be in the message or found again on disk.

So pi-agent-team is for agents that keep working and talk along the way. A2A is
for handing a self-contained job to an agent and getting a result back.

| | pi-agent-team | pi-a2a |
|---|---|---|
| Who handles an incoming message | the running session, at its next turn | a new isolated session per task |
| Who it can talk to | Pi nodes running this extension | any A2A agent: Pi, Hermes, Google ADK, LangChain, CrewAI |
| Addressing | name, list, `@label`, `*` | peer name or URL; `a2a_orchestrate` fans out by advertised capability |
| Finding peers on other machines | broker roster, mesh seeds or SWIM gossip, over the tailnet | static `peers` config, mDNS on one LAN, or a self-hosted a2a-switchboard gateway |
| Stopping two agents looping | `re` marks a reply, and a reply is never answered | a turn cap per conversation (default 5) |
| Long jobs | the sender does not block; the answer arrives later as a `team_send` | `returnImmediately` acks at once, `a2a_status` polls by task id |
| Following a peer | `/team watch` shows its turn output without waking you | none |
| Auth | one shared team token | shared or per-peer tokens, plus audit log, rate limits, inbound injection filtering and outbound redaction |

Compared against pi-a2a 0.7.13.

### Use pi-agent-team when

- every agent is Pi, on machines you control, connected by a private network.
- the message must reach the session doing the work. "How far along is the
  migration?" only makes sense to the session running it.
- you split work by role and address it by label, such as `@review` or
  `@runner`.
- you want to follow another agent's conclusions with `/team watch` without
  starting a turn of your own.

### Use pi-a2a when

- the other agent is not Pi, or not yours: Hermes, Google ADK, LangChain,
  CrewAI, anything that speaks A2A.
- the job is self-contained and you want a result, not a conversation.
- the job runs longer than you want to wait, and you would rather poll it by
  task id.
- callers need separate identities, an audit trail or rate limits.

### Both at once

The tool names do not overlap (`team_*` and `a2a_*`), and pi-a2a's inbound
server stays off until you start it. One workable split is pi-agent-team
between your own Pi nodes, and pi-a2a's outbound `a2a_call` for agents outside
the team.

## Install

```bash
pi install npm:@yiki21/pi-agent-team
```

Or from GitHub:

```bash
pi install git:github.com/Yiki21/pi-agent-team
```

Requires Node 22+ and Pi. No runtime dependencies. Everything below assumes this
is installed — there is no path to type.

## The three modes

All three share one API and pass one conformance suite, so they behave the same.
Pick one at a time.

| mode | how nodes find each other | how messages travel | needs |
|---|---|---|---|
| `broker` **(default)** | a broker you run pushes the roster | through the broker | a reachable broker URL |
| `mesh` | nodes exchange member tables directly | direct node-to-node | one seed address |
| `swim` | SWIM gossip (Go sidecar) | direct node-to-node | a seed, plus the sidecar built |

**Start with `broker`.** It is one process, it survives nodes coming and going,
and nothing needs to know anyone else's address in advance.

Use `mesh` when you want no centre and every node can reach every other. Use
`swim` when nodes actually die without saying goodbye and you need the roster to
notice.

## Use it

You need a token first. Anyone can generate one; everyone shares it.

```bash
openssl rand -hex 32
```

### Option A — broker

Run the broker on one always-on machine. It does not need Pi, only Node:

```bash
TEAM_TOKEN=<token> npx -y -p @yiki21/pi-agent-team pi-agent-team-broker --bind "$(tailscale ip -4)"
```

`pi install` puts the package somewhere that is not on your `PATH`, so the
broker command is run through `npx` rather than by name.

**For a permanent broker, use systemd** — a broker started from a terminal dies
when you close it, and one in systemd comes back after a reboot. A unit file
plus the reasoning is in [docs/systemd.md](docs/systemd.md).

Then each node:

```
/team join dev --url http://<tailnet-ip>:8787 --token <token>
```

### Option B — mesh or swim

No broker. The first node starts, everyone else points at it:

```
# first node
/team join dev --mode mesh --token <token>

# the address to hand out is printed by:
/team status

# every other node
/team join dev --mode mesh --seeds 100.64.0.1:19801 --token <token> --name dev01-web
```

For `swim`, add `--mode swim`. It also needs a sidecar, which you build once:

```bash
cd ~/.pi/agent/npm/node_modules/@yiki21/pi-agent-team/swim
go build -o ../swim-sidecar .
```

`swim` seeds use the **gossip** port, not the delivery port. `/team status`
prints the right one.

### Then talk

```
/team                     # interactive menu
/team peers               # who is online
/team send other hello
/team send "@web" deploy is starting
/team send '*' maintenance in 5 minutes
```

## Configure

Every option works three ways. They share one implementation, so what you can
set one way you can set every way.

**Startup — environment variables.** For scripts, containers, systemd:

```bash
TEAM_NAME=dev01-web \
TEAM_MODE=broker \
TEAM_URL=http://100.64.0.1:8787 \
TEAM_TOKEN=<token> \
TEAM_LABELS=web \
  pi
```

**Commands — for you.** Options are the same names:

```
/team join dev --url http://100.64.0.1:8787 --token <token> --name dev01-web
/team mode mesh
/team join dev --mode mesh --seeds 100.64.0.1:19801
```

**Tools — for the model and automation.** `team_join`, `team_info`,
`team_roster`, `team_send`, `team_label`, `team_leave`. `team_join` takes the
same options as `/team join`.

| Option | Meaning | Saved? |
|---|---|---|
| `url` | broker address (broker mode) | yes |
| `token` | team token | yes |
| `mode` | `broker` / `mesh` / `swim` | yes |
| `seeds` | mesh/swim seed addresses | yes |
| `name` | this node's name | no |
| `labels` | this node's labels, for `@label` sends | no |
| `port` | mesh/swim listening port (0 = pick one) | no |
| `listen` | mesh/swim listening address | no |

Reply behaviour is a separate setting, not a connection option:

| `reply` | What it does |
|---|---|
| `off` | Never reminds. Messages still arrive; answering is the model's call |
| `remind` *(default)* | Reminds once when a request goes unanswered |
| `mirror` | Mirrors every turn's output to all nodes as `fyi` (both sides on means both keep posting) |

Set it with `reply=` / `TEAM_REPLY` / `--team-reply`, or `/team reply <mode>` at
runtime. The older names still work: `TEAM_ANNOUNCE`, `--team-announce`,
`/team announce`, and the values `auto` (now `remind`) and `always` (now
`mirror`). Using one prints a note saying what it is called now — `auto` and
`always` no longer describe what the mode does, which is why they were renamed.

The last four are not saved because one machine can run several Pi agents and
they share one config file. Saving the name would have the second agent
overwrite the first; saving the port would have it try to listen on a port
already in use. Pass them on each run.

```
/team join dev --mode mesh --seeds 100.64.0.1:19801 --name dev01-web --port 19801
```

`/team mode` switches mode without retyping `url` or `token`.

## Watching a peer's answers

`reply` is about how *you* answer others. Subscribing is the inverse: it is how you
follow what a specific peer concludes, without that peer doing anything differently.

```
/team watch add dev02      # follow dev02's answers
/team watch list
/team watch remove dev02
```

the model-facing equivalent is `team_watch({ action, target })`.

What you get: after each of `dev02`'s turns, its final output arrives as a `SUB` card
with the first 200 characters. What you do **not** get: a new turn. The card never wakes
your model, so reading it is something you do when you next run. That is also what keeps
two mutually-subscribed nodes from looping — the notification cannot start anything.

Things worth knowing:

- **Broker mode only.** Mesh and swim have no broker to hold the subscription list, so
  `team watch` fails with a reason rather than silently doing nothing.
- **The peer is not told.** It receives no receipt or acknowledgement of any kind, and a
  publisher that nobody is watching is told so. There is no way for a peer to detect that
  it is being watched.
- **At most 8.** Rejected one at a time so you can choose what to drop; nothing is evicted
  silently.
- **Not persisted.** A broker restart drops subscriptions; each subscriber re-registers on
  reconnect. Notifications during the gap are lost, the same way messages in flight are.
- **Summaries are bounded and merged.** A peer emitting in a tight loop cannot flood a
  subscriber: notifications are rate-limited and the skipped ones are reported as a count
  on the next one, never dropped silently.

`reply=off` does not disable subscription publishing. The two settings are independent.

## How it works

[`docs/how-it-works.md`](docs/how-it-works.md) covers:

- the three transports in detail, and which to pick
- what a message flows through, and the rules that end a conversation
- SWIM mode: why the sidecar is Go, and its two ports
- the security model, and what a stolen token gets an attacker
- design notes that were measured rather than assumed
- how to run the tests
- known limitations

## Requirements

- Node 22+
- Pi
- [Tailscale](https://tailscale.com/) or any private network — the broker binds
  a single non-public address
- Go, only for `swim` mode

## License

MIT
