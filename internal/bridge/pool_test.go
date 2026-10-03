package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"qoder2api/account"
	"qoder2api/internal/cosy"
)

func testSlot(t *testing.T, id, userType string) *Slot {
	t.Helper()
	account.SetDataRoot(t.TempDir())
	sess, err := cosy.NewSession(
		cosy.AuthIdentity{Uid: id, UserType: userType},
		"mid-"+id, "mtok-"+id, "mtype-"+id,
	)
	if err != nil {
		t.Fatalf("NewSession(%s): %v", id, err)
	}
	return NewSlot(id, id, account.RegionCN, sess, NewBearerClient(sess))
}

func TestPoolPickSkipsCooling(t *testing.T) {
	p := NewPool(testSlot(t, "a", "t"), testSlot(t, "b", "t"))
	s1, ok := p.Pick(nil)
	if !ok || s1.ID != "a" {
		t.Fatalf("first pick = %v", s1)
	}
	p.Cool("a", time.Minute, "quota")
	s2, ok := p.Pick(nil)
	if !ok || s2.ID != "b" {
		t.Fatalf("after cooling a, pick = %v", s2)
	}
	p.Cool("b", time.Minute, "quota")
	if _, ok := p.Pick(nil); ok {
		t.Fatal("expected no available account when all cooling")
	}
}

func TestPoolPickRespectsExclude(t *testing.T) {
	p := NewPool(testSlot(t, "a", "t"), testSlot(t, "b", "t"))
	if s, ok := p.Pick(map[string]struct{}{"a": {}}); !ok || s.ID != "b" {
		t.Fatalf("exclude a -> %v", s)
	}
	if _, ok := p.Pick(map[string]struct{}{"a": {}, "b": {}}); ok {
		t.Fatal("all excluded should yield none")
	}
}

func TestPoolCooldownExpires(t *testing.T) {
	p := NewPool(testSlot(t, "a", "t"))
	p.Cool("a", 20*time.Millisecond, "transient")
	if _, ok := p.Pick(nil); ok {
		t.Fatal("should be cooling")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := p.Pick(nil); !ok {
		t.Fatal("cooldown should have expired")
	}
}

func TestShouldFailoverClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"403 quota", NewUpstreamError(403, `{"code":"112"}`), true},
		{"429 rate", NewUpstreamError(429, "usage exceeds frequency limit"), true},
		{"502 transient", NewUpstreamError(502, "provider_error"), true},
		{"400 content policy", NewUpstreamError(400, "InternalError.Algo.DataInspectionFailed"), false},
		{"400 invalid param", NewUpstreamError(400, "invalid_parameter_error: Range of x"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := ShouldFailover(c.err); got != c.want {
			t.Errorf("%s: ShouldFailover=%v want %v", c.name, got, c.want)
		}
	}
}

func TestCooldownForClassification(t *testing.T) {
	if got := CooldownFor(NewUpstreamError(403, "quota")); got != CooldownQuota {
		t.Errorf("403 -> %v", got)
	}
	if got := CooldownFor(NewUpstreamError(429, "rate")); got != CooldownRate {
		t.Errorf("429 -> %v", got)
	}
	if got := CooldownFor(NewUpstreamError(502, "boom")); got != CooldownTransient {
		t.Errorf("502 -> %v", got)
	}
}

func poolBridge(t *testing.T, url string, ids ...string) (*Bridge, *Pool) {
	t.Helper()
	slots := make([]*Slot, 0, len(ids))
	for _, id := range ids {
		slots = append(slots, testSlot(t, id, "type_"+id))
	}
	pool := NewPool(slots...)
	b := NewPoolBridge(pool, map[string]interface{}{
		"model_config": map[string]interface{}{"key": "auto"},
		"messages":     []interface{}{},
	})
	b.chatStreamURL = url
	return b, pool
}

// 第一个账号 403（额度）→ 冷却换号 → 第二个账号 200 成功。
func TestCallQoderFailsOverToNextAccount(t *testing.T) {
	hits := 0
	var userTypes []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		raw, _ := io.ReadAll(r.Body)
		if plain, err := cosy.Decode(string(raw)); err == nil {
			var m map[string]interface{}
			if json.Unmarshal(plain, &m) == nil {
				if ut, ok := m["aliyun_user_type"].(string); ok {
					userTypes = append(userTypes, ut)
				}
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if hits == 1 {
			fmt.Fprint(w, sseEnvelope(403, `{"code":"112","message":"need upgrade"}`))
			return
		}
		fmt.Fprint(w, sseContent("ok"))
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	b, pool := poolBridge(t, srv.URL, "a", "b")
	var got []string
	err := b.CallQoderWithOpts(context.Background(), "codex", testMessages(), "auto", nil, CallOpts{},
		func(d Delta) {
			if d.Content != "" {
				got = append(got, d.Content)
			}
		})
	if err != nil {
		t.Fatalf("expected failover success, got %v", err)
	}
	if hits != 2 {
		t.Fatalf("upstream hits = %d, want 2", hits)
	}
	if len(got) != 1 || got[0] != "ok" {
		t.Fatalf("deltas = %v", got)
	}
	if len(userTypes) != 2 || userTypes[0] != "type_a" || userTypes[1] != "type_b" {
		t.Fatalf("expected failover a->b, userTypes=%v", userTypes)
	}
	if _, ok := pool.Pick(map[string]struct{}{"b": {}}); ok {
		t.Fatal("account a should be cooling after 403")
	}
}

// 所有账号都 403 → 每个账号各试一次后返回错误，且全部进入冷却。
func TestCallQoderAllAccountsFail(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseEnvelope(403, `{"code":"112","message":"need upgrade"}`))
	}))
	defer srv.Close()

	b, pool := poolBridge(t, srv.URL, "a", "b")
	err := b.CallQoderWithOpts(context.Background(), "codex", testMessages(), "auto", nil, CallOpts{}, func(Delta) {})
	if err == nil {
		t.Fatal("expected error when all accounts fail")
	}
	if hits != 2 {
		t.Fatalf("hits = %d, want 2 (one per account)", hits)
	}
	if _, ok := pool.Pick(nil); ok {
		t.Fatal("both accounts should be cooling")
	}
}

// 内容审核（不可换号）→ 只打一次上游，不换号。
func TestCallQoderContentPolicyNoFailover(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, sseEnvelope(500, "InternalError.Algo.DataInspectionFailed: bad content"))
	}))
	defer srv.Close()

	b, _ := poolBridge(t, srv.URL, "a", "b")
	err := b.CallQoderWithOpts(context.Background(), "codex", testMessages(), "auto", nil, CallOpts{}, func(Delta) {})
	if err == nil {
		t.Fatal("expected content policy error")
	}
	if hits != 1 {
		t.Fatalf("hits = %d, want 1 (no failover on content policy)", hits)
	}
}
