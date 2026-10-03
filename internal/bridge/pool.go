package bridge

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"qoder2api/account"
	"qoder2api/internal/cosy"
	"qoder2api/logger"
)

// 最小档账号池：选号 + 冷却 + 失败换号。
// 不做加权 / 熔断 / 降权 / 成本探索（这些留待后续按需增加）。

// ErrNoAccount 池中没有任何可用账号。
var ErrNoAccount = errors.New("no available account in pool")

const (
	// CooldownQuota 额度 / 权限类失败（上游 401/403，如 code 112 需要升级套餐）：
	// 账号在当前计费周期内基本不可用，给长冷却。
	CooldownQuota = 30 * time.Minute
	// CooldownRate 上游频控（429）。
	CooldownRate = 60 * time.Second
	// CooldownTransient 瞬时上游故障（418/5xx/传输抖动）同账号重试耗尽后：
	// 短冷却，避免立刻再撞同一个账号。
	CooldownTransient = 15 * time.Second
)

// Slot 是一个可用账号的运行时状态（session + 冷却）。
type Slot struct {
	ID     string
	Name   string
	Region account.Region

	sess   *cosy.SessionContext
	client *BearerClient

	mu            sync.Mutex
	cooldownUntil time.Time
	lastErr       string
}

// NewSlot 组装一个池内账号。
func NewSlot(id, name string, region account.Region, sess *cosy.SessionContext, client *BearerClient) *Slot {
	return &Slot{ID: id, Name: name, Region: region, sess: sess, client: client}
}

func (s *Slot) available(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !now.Before(s.cooldownUntil)
}

func (s *Slot) cool(d time.Duration, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	until := time.Now().Add(d)
	if until.After(s.cooldownUntil) {
		s.cooldownUntil = until
	}
	s.lastErr = reason
}

func (s *Slot) noteSuccess() {
	s.mu.Lock()
	s.lastErr = ""
	s.mu.Unlock()
}

// Pool 是账号池：轮询选号，跳过冷却中与已试过的账号。
type Pool struct {
	mu     sync.Mutex
	slots  []*Slot
	cursor int
}

func NewPool(slots ...*Slot) *Pool { return &Pool{slots: slots} }

func (p *Pool) Add(s *Slot) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots = append(p.slots, s)
}

func (p *Pool) Size() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.slots)
}

// snapshot 返回账号切片的副本（供 Bridge 遍历，不持有锁）。
func (p *Pool) snapshot() []*Slot {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Slot, len(p.slots))
	copy(out, p.slots)
	return out
}

// Pick 从轮询游标开始扫描，返回第一个既未冷却也不在 exclude 中的账号。
func (p *Pool) Pick(exclude map[string]struct{}) (*Slot, bool) {
	if p == nil {
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.slots)
	if n == 0 {
		return nil, false
	}
	now := time.Now()
	start := p.cursor
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		s := p.slots[idx]
		if _, skip := exclude[s.ID]; skip {
			continue
		}
		if !s.available(now) {
			continue
		}
		p.cursor = (idx + 1) % n
		return s, true
	}
	return nil, false
}

// Cool 给指定账号设置冷却（取较晚的到期时间）。
func (p *Pool) Cool(id string, d time.Duration, reason string) {
	for _, s := range p.snapshot() {
		if s.ID == id {
			s.cool(d, reason)
			logger.Info("pool: account %s cooled for %s (%s)", id, d, reason)
			return
		}
	}
}

// NoteSuccess 清除账号的失败标记。
func (p *Pool) NoteSuccess(id string) {
	for _, s := range p.snapshot() {
		if s.ID == id {
			s.noteSuccess()
			return
		}
	}
}

// ShouldFailover 判断某次上游失败是否值得换号。
// 只有「换号可能成功」的错误才换：额度/权限(401/403)、频控(429)、
// 瞬时上游故障(418/5xx/传输层)。客户端参数错、内容审核换号无意义，直接上抛。
func ShouldFailover(err error) bool {
	if err == nil {
		return false
	}
	var ue *UpstreamError
	if errors.As(err, &ue) {
		if ue.ErrType == ErrTypeContentPolicy {
			return false
		}
		switch ue.Status {
		case http.StatusUnauthorized, http.StatusForbidden: // 额度 / 凭证
			return true
		case http.StatusTooManyRequests: // 频控
			return true
		}
		if ue.Status == 418 || ue.Status >= 500 {
			return true
		}
		if ue.Status >= 400 && ue.Status < 500 {
			return false // 其余 4xx：客户端参数错
		}
		// 流内业务错误（Status=0）：额度/频控类才换号
		return isAccountExhaustedDetail(ue.Detail)
	}
	// 裸传输错误（TLS EOF / 超时等）
	return IsTransientTransport(err)
}

// isAccountExhaustedDetail 识别流内业务错误里「该账号已耗尽」的语义。
func isAccountExhaustedDetail(detail string) bool {
	d := strings.ToLower(detail)
	for _, k := range []string{"quota", "credit", "exceed", "10605", "rate limit", "too many request"} {
		if strings.Contains(d, k) {
			return true
		}
	}
	return false
}

// CooldownFor 给出错误对应的冷却时长。
func CooldownFor(err error) time.Duration {
	var ue *UpstreamError
	if errors.As(err, &ue) {
		switch {
		case ue.Status == http.StatusUnauthorized || ue.Status == http.StatusForbidden:
			return CooldownQuota
		case ue.Status == http.StatusTooManyRequests:
			return CooldownRate
		default:
			return CooldownTransient
		}
	}
	return CooldownTransient
}
