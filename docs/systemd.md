# Running the broker under systemd

The broker is a long-running process, so it wants an init system to start it at
boot and restart it if it dies. `RESTART=always` in a shell script will not
survive a reboot.

Two ways, and the difference matters:

| | system unit (**recommended**) | user unit |
|---|---|---|
| Starts at boot | yes | only with `linger` enabled |
| Survives logout | yes | **no**, unless `linger` is on |
| Runs as | a dedicated user | you, reading your home directory |
| Needs root once | to install the unit | no — but `linger` needs root anyway |

A broker is shared infrastructure: it stays up when you are not logged in. That
is a system unit. Use a user unit when you are trying it out on a laptop.

Everything below was verified by running it. Where a value looks arbitrary, the
reason is given.

## System unit

### 1. A user for it to run as

```bash
sudo useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin pi-team
```

### 2. Where the code and the token live

```bash
sudo mkdir -p /usr/lib/pi-agent-team
# Copy broker.mjs and the src/ directory next to it:
#   /usr/lib/pi-agent-team/broker.mjs
#   /usr/lib/pi-agent-team/src/ws.js
# Either from a clone, or out of the npm package:
#   npm pack @yiki21/pi-agent-team && tar xzf yiki21-pi-agent-team-*.tgz
sudo cp -r broker.mjs src /usr/lib/pi-agent-team/
```

The broker needs `broker.mjs` and `src/ws.js` and nothing else. It has no
dependencies to install.

Then the token. **Nothing else needs its own file:** use the same token every
node uses — the one `/team create` printed, or the one already in
`~/.pi/agent/pi-agent-team/<team>.json`.

```bash
sudo install -d -m 0700 -o pi-team -g pi-team /etc/pi-agent-team
printf '%s' 'REPLACE_WITH_YOUR_TOKEN' | sudo tee /etc/pi-agent-team/token >/dev/null
sudo chown pi-team:pi-team /etc/pi-agent-team/token
sudo chmod 0600 /etc/pi-agent-team/token
```

`printf`, not `echo`: `echo` appends a newline, and a token with a trailing
newline is a different token. The broker will reject every node with a 401, and
the fingerprints will differ by one byte you cannot see.

### 3. The unit

```bash
sudo tee /etc/systemd/system/pi-agent-team-broker.service >/dev/null <<'EOF'
[Unit]
Description=pi-agent-team broker (tailnet only)
Documentation=https://github.com/Yiki21/pi-agent-team
After=network-online.target tailscaled.service
Wants=network-online.target

[Service]
Type=simple
User=pi-team
Group=pi-team
LoadCredential=team-token:/etc/pi-agent-team/token
ExecStart=/usr/bin/node /usr/lib/pi-agent-team/broker.mjs --bind 100.99.85.111 --port 8787
Restart=on-failure
RestartSec=3
RestartPreventExitStatus=78
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
CapabilityBoundingSet=
AmbientCapabilities=

[Install]
WantedBy=multi-user.target
EOF
```

Two lines to change, and both matter:

- **`ExecStart=... /usr/bin/node`** — an absolute path. A system unit gets
  `PATH=/usr/local/bin:/usr/bin`, which is not your shell's `PATH`. On a machine
  where node comes from Homebrew, nix, or fnm, `node` will not resolve and the
  unit fails with `status=203/EXEC`. Check with `command -v node` and use what
  it prints.

- **`--bind`** — the tailnet address, from `tailscale ip -4`. The broker refuses
  `0.0.0.0` on purpose: it has no TLS of its own and expects to sit behind
  WireGuard.

The hardening directives are safe for this process specifically: the broker
reads no files after startup and writes none at all, so `ProtectHome` and
`ProtectSystem=strict` cost it nothing. `systemd-analyze verify` reports no
issues for the unit above.

### 4. Start it

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pi-agent-team-broker
systemctl status pi-agent-team-broker --no-pager
```

Expect this in the journal:

```
broker 监听 ws://100.99.85.111:8787
token 指纹 1a2b3c4d(日志核对用,不是 token 本身)
接管模式 开(同名新连接踢旧连接)
```

That fingerprint is the first 8 hex characters of `sha256(token)`. It exists so
you can compare two sides without printing the token. If a node is refused, its
message shows both fingerprints — equal means the token is not the problem.

Check it answers:

```bash
curl -s http://"$(tailscale ip -4)":8787/health
# {"ok":true,"count":0,"peers":[],"members":[]}
```

## The token, done three ways

All three were run; pick one.

**`LoadCredential`** (as above) — the token is not in the unit file's
environment, so it does not show up in `systemctl show -p Environment`, in
`/proc/<pid>/environ`, or in a crash dump. Recommended.

**`EnvironmentFile`** — simpler, and it works, but the token appears in the
process environment:

```ini
EnvironmentFile=/etc/pi-agent-team/broker.env
```

```
TEAM_TOKEN=REPLACE_WITH_YOUR_TOKEN
```

The file must contain `KEY=value`. Pointing `EnvironmentFile` at a file holding
a bare token (which is how some other services take secrets) does not work and
does not say why: systemd yields an empty variable, and the broker then exits
with "拒绝启动:必须设置 TEAM_TOKEN" — a message that points at the token being
missing, not at the file being the wrong shape. Verified.

**Inline** — for a throwaway test only. `systemctl show` will print it to anyone
who can run that command:

```ini
Environment=TEAM_TOKEN=REPLACE_WITH_YOUR_TOKEN
```

## Restart behaviour

`Restart=on-failure` restarts the broker if it crashes. Verified: killing the
process with `SIGKILL` brings it back under a new PID within a few seconds, with
`NRestarts` incremented.

`RestartPreventExitStatus=78` is the part worth having. The broker exits with
**78** (`EX_CONFIG`, from `sysexits(3)`) when it cannot bind — port already in
use, address not on this machine, no permission. Restarting cannot fix any of
those, so systemd stops instead of looping. Verified with another process
holding the port:

```
Main process exited, code=exited, status=78/CONFIG
Failed with result 'exit-code'
NRestarts: 0
```

Without that line you get a restart every 3 seconds and a journal that fills
with the same message.

The broker's own message on a port clash names the address and port, tells you
how to find the holder, and points at the usual cause: another broker already
running on that machine.

### Stopping

`systemctl stop` sends `SIGTERM`. The broker closes every client with WebSocket
code **1001** (Going Away), so nodes can tell "the broker is being restarted"
apart from "the network died". Verified: a connected client sees `1001`, and the
stop completes in about 0.03s.

Nodes reconnect on their own with exponential backoff. Messages in flight during
the restart are lost; there is no queue.

## User unit

For a laptop, or to try it without root:

```bash
install -d -m 0700 ~/.config/pi-agent-team
printf '%s' 'YOUR_TOKEN' > ~/.config/pi-agent-team/token
chmod 0600 ~/.config/pi-agent-team/token

mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/pi-agent-team-broker.service <<'EOF'
[Unit]
Description=pi-agent-team broker
After=network-online.target

[Service]
Type=simple
LoadCredential=team-token:%h/.config/pi-agent-team/token
ExecStart=/usr/bin/node %h/.local/lib/pi-agent-team/broker.mjs --bind 127.0.0.1 --port 8787
Restart=on-failure
RestartSec=3
RestartPreventExitStatus=78

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now pi-agent-team-broker
```

To survive logout, you must also enable lingering:

```bash
sudo loginctl enable-linger "$USER"
loginctl show-user "$USER" --property=Linger   # Linger=yes
```

Without it the broker stops when your last session ends — you ssh in, enable it,
disconnect, and the team goes down. Note that this needs root anyway, which
removes much of the reason to prefer a user unit.

A user unit inherits a different environment than your interactive shell. Node
from Homebrew or nix may not be on its `PATH`, and if it is, it may be a
different version than the one in your terminal. Use an absolute path here too.

## Firewall

The broker listens on the tailnet interface only. If the tailnet has an ACL that
does not allow port 8787 between the nodes, connections time out with no useful
error on either side — the client just reports it cannot connect. Test from
another node:

```bash
curl -s --max-time 3 http://100.99.85.111:8787/health
```

## Rotating the token

```bash
# 1. New token on the broker
printf '%s' "$(openssl rand -hex 32)" | sudo tee /etc/pi-agent-team/token >/dev/null
sudo systemctl restart pi-agent-team-broker

# 2. Every node, with the value the broker just logged
/team join <team> --token <new token>
```

There is no overlap window: nodes fail with 401 until they are updated. The
client reports that as a token mismatch, showing both fingerprints, rather than
as a generic connection failure.
