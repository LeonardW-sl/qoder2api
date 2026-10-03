package bridge

import (
	"time"

	"qoder2api/account"
)

// NewSlotFromSecret 用账号凭证构建一个可入池的 Slot（复用 NewBridge 的
// 会话构建逻辑：OAuth 直接 userinfo，PAT 走 jobToken 交换 + 稳定指纹）。
func NewSlotFromSecret(id, name string, region account.Region, secret string) (*Slot, error) {
	b, err := NewBridge(secret, region, nil)
	if err != nil {
		return nil, err
	}
	return NewSlot(id, name, region, b.sess, b.client), nil
}

// NewPoolBridge 用账号池创建桥接（多账号轮询 + 失败换号）。
func NewPoolBridge(pool *Pool, templateBase map[string]interface{}) *Bridge {
	return &Bridge{pool: pool, templateBase: templateBase}
}

// candidateSlots 返回当前可参与选号的账号：池优先；无池时退化为单账号
// （保持既有单账号构造方式与测试兼容）。
func (b *Bridge) candidateSlots() []*Slot {
	if b.pool != nil {
		return b.pool.snapshot()
	}
	if b.sess == nil || b.client == nil {
		return nil
	}
	return []*Slot{{ID: "default", Name: "default", Region: b.region, sess: b.sess, client: b.client}}
}

// pickSlot 选一个账号，跳过 exclude 与冷却中的账号。
func (b *Bridge) pickSlot(exclude map[string]struct{}) (*Slot, bool) {
	if b.pool != nil {
		return b.pool.Pick(exclude)
	}
	slots := b.candidateSlots()
	if len(slots) == 0 {
		return nil, false
	}
	if _, skip := exclude[slots[0].ID]; skip {
		return nil, false
	}
	return slots[0], true
}

// coolSlot 冷却账号（仅池模式下生效；单账号模式为 no-op）。
func (b *Bridge) coolSlot(id string, d time.Duration, reason string) {
	if b.pool != nil {
		b.pool.Cool(id, d, reason)
	}
}

// noteSlotSuccess 记录成功（仅池模式下生效）。
func (b *Bridge) noteSlotSuccess(id string) {
	if b.pool != nil {
		b.pool.NoteSuccess(id)
	}
}
