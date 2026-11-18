//go:build dur053_k6

package k6hook

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"durable-agent-execution-engine/internal/k4observe"
)

type message struct {
	Event    string   `json:"event"`
	Boundary string   `json:"boundary"`
	Identity Identity `json:"identity"`
	Fields   Fields   `json:"fields"`
	HookID   string   `json:"hook_id"`
	Nonce    string   `json:"nonce"`
	Arm      string   `json:"arm"`
	Sequence int      `json:"sequence"`
	Hash     string   `json:"sha256"`
	Journal  string   `json:"journal_sha256"`
}

type response struct {
	Command  string `json:"command"`
	Arm      string `json:"arm"`
	Sequence int    `json:"sequence"`
	Hash     string `json:"sha256"`
	Journal  string `json:"journal_sha256"`
	Until    int64  `json:"until_ms"`
}

type client struct {
	identity Identity
	address  string
	token    string
	http     *http.Client
}

func nonce() (string, error) {
	var raw [16]byte
	_, err := rand.Read(raw[:])
	return hex.EncodeToString(raw[:]), err
}

func FromEnvironment(actor, session string) (Client, error) {
	address := os.Getenv("DUR053_K6_HOOK_URL")
	if address == "" {
		return nil, nil
	}
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || address != "https://"+u.Host {
		return nil, errors.New("K6_HOOK_ADDRESS")
	}
	ca, err := os.ReadFile(os.Getenv("DUR053_K6_HOOK_CA_FILE"))
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("K6_HOOK_CA")
	}
	token := os.Getenv("DUR053_K6_HOOK_TOKEN")
	uid := os.Getenv("POD_UID")
	process, err := k4observe.ProcessStartID()
	if len(token) != 64 || uid == "" || err != nil {
		return nil, errors.New("K6_HOOK_IDENTITY")
	}
	if session == "" {
		session, err = nonce()
		if err != nil {
			return nil, errors.New("K6_HOOK_NONCE")
		}
	}
	c := &client{Identity{actor, uid, process, session}, address, token, &http.Client{
		Timeout:       2 * time.Second,
		Transport:     &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	if reply, _, err := c.send(message{Event: "register", Identity: c.identity}); err != nil || reply.Command != "registered" {
		return nil, errors.New("K6_HOOK_REGISTRATION")
	}
	return c, nil
}

func (c *client) send(m message) (response, string, error) {
	return c.sendContext(context.Background(), m)
}

func (c *client) sendContext(ctx context.Context, m message) (response, string, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return response{}, "", errors.New("K6_HOOK_ENCODE")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.address+"/v1/hook", bytes.NewReader(raw))
	if err != nil {
		return response{}, "", errors.New("K6_HOOK_REQUEST")
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	reply, err := c.http.Do(request)
	if err != nil {
		return response{}, "", errors.New("K6_HOOK_TRANSPORT")
	}
	defer reply.Body.Close()
	var r response
	if reply.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(reply.Body, 4096)).Decode(&r) != nil {
		return response{}, "", errors.New("K6_HOOK_REFUSED")
	}
	sum := sha256.Sum256(raw)
	return r, hex.EncodeToString(sum[:]), nil
}

func matchingACK(r response, hash string, now time.Time) bool {
	until := time.UnixMilli(r.Until)
	journal, err := hex.DecodeString(r.Journal)
	return r.Command == "ack" && r.Sequence > 0 && r.Hash == hash && r.Arm != "" &&
		err == nil && len(journal) == 32 && until.After(now) && !until.After(now.Add(60*time.Second))
}

func (c *client) Hit(boundary string, fields Fields) (Hold, error) {
	hook, err := nonce()
	if err != nil {
		return nil, err
	}
	hit, err := nonce()
	if err != nil {
		return nil, err
	}
	m := message{Event: "boundary_reached", Boundary: boundary, Identity: c.identity, Fields: fields, HookID: hook, Nonce: hit}
	r, hash, err := c.send(m)
	if err != nil || r.Command == "pass" {
		return nil, err
	}
	if !matchingACK(r, hash, time.Now()) {
		return nil, errors.New("K6_HOOK_ACK")
	}
	m.Arm, m.Sequence, m.Hash, m.Journal = r.Arm, r.Sequence, r.Hash, r.Journal
	now := time.Now()
	deadline := now.Add(time.UnixMilli(r.Until).Sub(now))
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	for ctx.Err() == nil {
		m.Event = "held"
		decision, _, err := c.sendContext(ctx, m)
		if err != nil {
			return nil, err
		}
		if decision.Arm != m.Arm || decision.Sequence != m.Sequence || decision.Hash != m.Hash || decision.Journal != m.Journal ||
			decision.Until != r.Until || !time.Now().Before(deadline) {
			break
		}
		switch decision.Command {
		case "release", "duplicate", "drop":
			return &hold{c, m, decision.Command, deadline}, nil
		case "wait":
			time.Sleep(min(75*time.Millisecond, time.Until(deadline)))
		default:
			return nil, errors.New("K6_HOOK_COMMAND")
		}
	}
	return nil, errors.New("K6_HOOK_EXPIRED")
}

type hold struct {
	client   *client
	message  message
	action   string
	deadline time.Time
}

func (h *hold) Action() string      { return h.action }
func (h *hold) Deadline() time.Time { return h.deadline }
func (h *hold) Finish() error {
	h.message.Event = "released"
	ctx, cancel := context.WithDeadline(context.Background(), h.deadline)
	defer cancel()
	r, _, err := h.client.sendContext(ctx, h.message)
	if err != nil || r.Command != "done" {
		return errors.New("K6_HOOK_FINISH")
	}
	return nil
}
