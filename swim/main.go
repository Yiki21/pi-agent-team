// pi-agent-team SWIM sidecar
//
// 为什么是 Go:memberlist 是 SWIM 最成熟的实现(Kubernetes、Consul、
// Cassandra 在用同一份)。SWIM 最难的三个部分 —— incarnation number、
// suspicion 超时、ping-req 间接探测 —— 恰恰是只在故障时才暴露的错误。
// 在抖动的网络上跑一个没写对的故障检测器,会得到满屏假离线告警。
//
// ── 职责边界 ──
//   它只负责:成员表、故障检测、成员变更通知。
//   它**不**负责消息投递 —— 应用消息走 Pi 节点之间的直连。
//
// ── 接口 ──
//   本地 HTTP(unix socket 或 127.0.0.1 TCP):
//     GET /members   当前成员快照
//     GET /events    SSE 流:joined / left / updated
//     GET /health   存活检查
//
// ── 认证 ──
//   memberlist 用自己的 gossip 密钥(AES-GCM)保护节点间通信;
//   本地 HTTP 只监听回环,不加额外认证。
//   team token 用来派生 gossip 密钥(见 -token 参数),
//   于是"能加入 team 的人"和"能加入 gossip"是同一批人。

package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/memberlist"
)

// Member 是暴露给 Node 侧的成员格式。
// 字段名和 Node 侧 transport 的 members() 对齐,减少一层映射。
type Member struct {
	Name    string   `json:"name"`
	Addr    string   `json:"addr"`
	Port    int      `json:"port"`
	Labels  []string `json:"labels"`
	Host    string   `json:"host"`
	State   string   `json:"state"`   // alive | suspect | dead | left
	Service string   `json:"service"` // 固定为 "pi-agent-team",便于区分
	// Deliver 是应用消息的直连端口(mesh 监听端口)。
	// SWIM 只管 gossip,消息不走它 —— 但成员表需要告诉对端
	// "该往哪个端口发消息",否则收件人知道名字却没有投递路径。
	Deliver int `json:"deliver"`
}

// Event 是 SSE 推给 Node 的变更通知。
type Event struct {
	Kind   string   `json:"kind"` // joined | left | updated | snapshot
	Member *Member  `json:"member,omitempty"`
	All    []Member `json:"all,omitempty"`
	At     string   `json:"at"`
}

type hub struct {
	mu      sync.Mutex
	clients map[chan Event]struct{}
}

func newHub() *hub {
	return &hub{clients: make(map[chan Event]struct{})}
}

func (h *hub) subscribe() chan Event {
	ch := make(chan Event, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *hub) unsubscribe(ch chan Event) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

// publish 是非阻塞的:慢客户端不能拖住成员变更的处理。
// 队列满了就丢这一条事件 —— 客户端下次拉 /members 会拿到全量快照,
// 所以丢事件不会导致状态永久不一致。
func (h *hub) publish(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- ev:
		default:
		}
	}
}

// delegate 把 memberlist 的成员变更回调转成事件。
type delegate struct {
	hub *hub
}

func nodeToMember(n *memberlist.Node) Member {
	m := Member{
		Name:    n.Name,
		Addr:    n.Addr.String(),
		Port:    int(n.Port),
		Service: "pi-agent-team",
		State:   stateName(n.State),
	}
	// memberlist 的 Meta 允许携带任意字节。我们用它传 host 和 labels,
	// 格式: "host\x00label1,label2"
	if len(n.Meta) > 0 {
		parts := splitMeta(n.Meta)
		if len(parts) >= 1 {
			m.Host = parts[0]
		}
		if len(parts) >= 2 && parts[1] != "" {
			m.Labels = splitComma(parts[1])
		}
		if len(parts) >= 3 && parts[2] != "" {
			if v, err := strconv.Atoi(parts[2]); err == nil {
				m.Deliver = v
			}
		}
	}
	return m
}

func (d *delegate) NotifyJoin(n *memberlist.Node) {
	m := nodeToMember(n)
	d.hub.publish(Event{Kind: "joined", Member: &m, At: time.Now().UTC().Format(time.RFC3339Nano)})
	log.Printf("joined: %s %s:%d", m.Name, m.Addr, m.Port)
}

func (d *delegate) NotifyLeave(n *memberlist.Node) {
	m := nodeToMember(n)
	d.hub.publish(Event{Kind: "left", Member: &m, At: time.Now().UTC().Format(time.RFC3339Nano)})
	log.Printf("left: %s", m.Name)
}

func (d *delegate) NotifyUpdate(n *memberlist.Node) {
	m := nodeToMember(n)
	d.hub.publish(Event{Kind: "updated", Member: &m, At: time.Now().UTC().Format(time.RFC3339Nano)})
	log.Printf("updated: %s", m.Name)
}

// metaPayload 由 main 填入,格式 "host\x00labels\x00deliverPort"
var metaPayload string

// NodeMeta 让 host 和 labels 随成员信息传播,这样其他节点不必额外查询。
func (d *delegate) NodeMeta(limit int) []byte {
	b := []byte(metaPayload)
	if len(b) > limit {
		if i := indexByte(b, 0); i >= 0 && i <= limit {
			return b[:i]
		}
		return b[:limit]
	}
	return b
}

// 下面四个方法是 memberlist.Delegate 接口的要求,但我们是刻意留空的:
// 应用消息走 Pi 节点之间的直连,不走 gossip 的用户数据通道。
// 所以这里没有实现内容,不是忘了实现。
func (d *delegate) NotifyMsg([]byte) {}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

func (d *delegate) LocalState(join bool) []byte { return nil }

func (d *delegate) MergeRemoteState(buf []byte, join bool) {}

var _ memberlist.Delegate = (*delegate)(nil)

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------- 参数

func main() {
	name := flag.String("name", "", "节点名(必填,和 Pi 侧 TEAM_NAME 一致)")
	bindAddr := flag.String("bind", "127.0.0.1", "gossip 绑定地址")
	bindPort := flag.Int("port", 7946, "gossip 端口(0 = 内核分配)")
	httpAddr := flag.String("http", "127.0.0.1:0", "本地 HTTP 监听地址")
	seeds := flag.String("seeds", "", "种子地址,逗号分隔 host:port")
	token := flag.String("token", "", "team token,用于派生 gossip 密钥(必填)")
	host := flag.String("host", "", "机器名,暴露给其他节点")
	labels := flag.String("labels", "", "标签,逗号分隔")
	deliver := flag.Int("deliver", 0, "应用消息的直连端口,随成员信息通告")
	flag.Parse()

	if *name == "" || *token == "" {
		fmt.Fprintln(os.Stderr, "-name 和 -token 都是必填")
		os.Exit(2)
	}

	// Meta:host\x00labels
	metaPayload = *host + "\x00" + *labels + "\x00" + strconv.Itoa(*deliver)

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = *name
	cfg.BindAddr = *bindAddr
	cfg.BindPort = *bindPort
	cfg.LogOutput = os.Stderr

	// 从 team token 派生 gossip 密钥。
	// 16/24/32 字节都合法(AES-128/192/256)。
	key := sha256.Sum256([]byte("pi-agent-team-gossip\x00" + *token))
	cfg.SecretKey = key[:]
	cfg.GossipVerifyIncoming = true
	cfg.GossipVerifyOutgoing = true

	h := newHub()
	cfg.Delegate = &delegate{hub: h}

	ml, err := memberlist.Create(cfg)
	if err != nil {
		log.Fatalf("memberlist 创建失败: %v", err)
	}
	defer ml.Shutdown()

	// 加入种子。失败不致命:节点仍可独立运行,后续有种子加入时会互相发现。
	if *seeds != "" {
		var addrs []string
		for _, s := range splitComma(*seeds) {
			if s != "" {
				addrs = append(addrs, s)
			}
		}
		if len(addrs) > 0 {
			if n, err := ml.Join(addrs); err != nil {
				log.Printf("警告:加入种子只成功 %d 个: %v", n, err)
			} else {
				log.Printf("已加入 %d 个种子", n)
			}
		}
	}

	// ---------------------------------------------------------------- HTTP

	mux := http.NewServeMux()

	snapshot := func() []Member {
		nodes := ml.Members()
		out := make([]Member, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, nodeToMember(n))
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out
	}

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"name":    ml.LocalNode().Name,
			"members": len(ml.Members()),
		})
	})

	mux.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"self":    ml.LocalNode().Name,
			"members": snapshot(),
		})
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("content-type", "text/event-stream")
		w.Header().Set("cache-control", "no-cache")
		w.Header().Set("connection", "keep-alive")

		ch := h.subscribe()
		defer h.unsubscribe(ch)

		// 先推一份全量快照,客户端不必额外拉一次
		writeSSE(w, Event{Kind: "snapshot", All: snapshot(), At: time.Now().UTC().Format(time.RFC3339Nano)})
		flusher.Flush()

		keepalive := time.NewTicker(20 * time.Second)
		defer keepalive.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case ev := <-ch:
				writeSSE(w, ev)
				flusher.Flush()
			case <-keepalive.C:
				fmt.Fprint(w, ": keepalive\n\n")
				flusher.Flush()
			}
		}
	})

	ln, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		log.Fatalf("HTTP 监听失败: %v", err)
	}

	// 把实际监听地址打到 stdout 的第一行 —— Node 侧靠它知道去哪儿连。
	// 格式固定,便于解析: "LISTEN http://127.0.0.1:PORT"
	fmt.Printf("LISTEN http://%s\n", ln.Addr().String())
	os.Stdout.Sync()

	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP 服务结束: %v", err)
		}
	}()

	// ---------------------------------------------------------------- 退出

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("关闭中")
	srv.Close()
	// Leave 让其他节点立刻知道我们走了,而不是等 suspicion 超时
	_ = ml.Leave(2 * time.Second)
}

func writeSSE(w http.ResponseWriter, ev Event) {
	b, _ := json.Marshal(ev)
	fmt.Fprintf(w, "data: %s\n\n", b)
}

func stateName(s memberlist.NodeStateType) string {
	switch s {
	case memberlist.StateAlive:
		return "alive"
	case memberlist.StateSuspect:
		return "suspect"
	case memberlist.StateDead:
		return "dead"
	case memberlist.StateLeft:
		return "left"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------- 小工具

func splitMeta(b []byte) []string {
	var out []string
	cur := ""
	for _, c := range string(b) {
		if c == 0 {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(c)
	}
	out = append(out, cur)
	return out
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
