package monitor

import (
	"sync"
	"time"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

type NodeDataSubscriber struct {
	Ch        chan *types.NodeData
	StartTime time.Time
}

type SubscriberManager struct {
	subscribers sync.Map
}

var (
	subscriberManagerInstance *SubscriberManager
	once                      sync.Once
)

func GetSubscriberManagerInstance() *SubscriberManager {
	once.Do(func() {
		subscriberManagerInstance = &SubscriberManager{}
	})
	return subscriberManagerInstance
}

func (r *SubscriberManager) AddSubscriber(taskName string, subscriber *NodeDataSubscriber) {
	r.subscribers.Store(taskName, subscriber)
}

func (r *SubscriberManager) DeleteSubscriber(taskName string) {
	r.subscribers.Delete(taskName)
}

func (r *SubscriberManager) Close() {
	r.subscribers.Range(func(key, value interface{}) bool {
		close(value.(*NodeDataSubscriber).Ch)
		return true
	})
}
