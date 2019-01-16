package stat

import (
	"github.com/blackbeans/turbo"
	"time"
)

//投递注册器
type DeliveryRegistry struct {
	registry *turbo.LRUCache //key为messageId-->value为过期时间
}

func NewDeliveryRegistry(capacity int) *DeliveryRegistry {
	tw := turbo.NewTimerWheel(100*time.Millisecond, 10)
	registry := turbo.NewLRUCache(capacity, tw, nil)
	return &DeliveryRegistry{registry: registry}
}

/*
*注册投递事件
**/
func (self DeliveryRegistry) Registe(messageId string, exp time.Duration) bool {
	now := time.Now()
	//过期或者不存在在直接覆盖设置
	expiredTime := now.Add(exp)
	exist, ok := self.registry.Get(messageId)
	if !ok || time.Time(exist.(time.Time)).Before(now) {
		self.registry.Put(messageId, expiredTime, exp)
		return true
	}

	return false
}

//取消注册
func (self DeliveryRegistry) UnRegiste(messageId string) {
	self.registry.Remove(messageId)
}
