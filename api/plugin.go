package api

type HookType uint8

const (
	PreStartHook HookType = iota
	PostStartHook
	PreCompletionHook
	PostCompletionHook
)

type PluginHandler func(*PluginContext)

// A plugin is a shared object that implements the Plugin interface
type Plugin interface {
	// Init the plugin
	Init() error

	// Get the plugin name
	Name() string

	// Get the plugin version
	Version() string

	/*
		Hook processing functions, general notes:
		@param ctx: The context of the plugin, which can store some data.
					The output of the plugin should be stored in the context.
		@param req: The original request of the hook.
		For all the hooks, the plugin should return an error if the hook fails
	*/
	PreStartHook(ctx *PluginContext)
	PostStartHook(ctx *PluginContext)
	PreCompletionHook(ctx *PluginContext)
	PostCompletionHook(ctx *PluginContext)
}
