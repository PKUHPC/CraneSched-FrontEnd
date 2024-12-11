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

func (r *SubscriberManager) AddSubscriber(taskID uint32, subscriber *NodeDataSubscriber) {
	r.subscribers.Store(taskID, subscriber)
}

func (r *SubscriberManager) DeleteSubscriber(taskID uint32) {
	r.subscribers.Delete(taskID)
}

func (r *SubscriberManager) Close() {
	r.subscribers.Range(func(key, value interface{}) bool {
		close(value.(*NodeDataSubscriber).Ch)
		return true
	})
}
