package api

// This file should be refactored for a better public/private separation,
// after we have a separate repo for plugind.

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"
)

type PluginContext struct {
	GrpcCtx context.Context
	Type    HookType
	Keys    map[string]any

	request  proto.Message
	index    uint8
	handlers []PluginHandler
	mu       sync.RWMutex
}

func (c *PluginContext) Set(key string, value any) {
	c.mu.Lock()
	c.Keys[key] = value
	c.mu.Unlock()
}

func (c *PluginContext) Get(key string) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Keys[key]
}

func (c *PluginContext) Request() proto.Message {
	return c.request
}

// This should only be called by the plugin daemon
func (c *PluginContext) Start() {
	c.index = 0
	for c.index < uint8(len(c.handlers)) {
		if c.handlers[c.index] == nil {
			// This shouldn't happen
			c.Abort()
			continue
		}
		c.handlers[c.index](c)
		c.index++
	}
}

// Plugin could call this to hand over the control to the next plugin.
// When this returned, the caller may continue.
func (c *PluginContext) Next() {
	c.index++
	for c.index < uint8(len(c.handlers)) {
		if c.handlers[c.index] == nil {
			// This shouldn't happen
			c.Abort()
			continue
		}
		c.handlers[c.index](c)
		c.index++
	}
}

// Plugin could call this to prevent the following plugins from being called.
func (c *PluginContext) Abort() {
	c.index = uint8(len(c.handlers))
}

func NewContext(ctx context.Context, req proto.Message, t HookType, hs *[]PluginHandler) *PluginContext {
	return &PluginContext{
		GrpcCtx:  ctx,
		Type:     t,
		Keys:     make(map[string]any),
		request:  req,
		index:    0,
		handlers: *hs,
		mu:       sync.RWMutex{},
	}
}
