package api

type HookType uint8

const (
	PreStartHook HookType = iota
	PostStartHook
	PreEndHook
	PostEndHook
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
		Hook processing functions:
			@param ctx: The context of the plugin. Request and other data could
						be accessed from the context.
			            The output of the plugin should be stored in the context.
		See PluginContext for details.
	*/
	PreStartHook(ctx *PluginContext)
	PostStartHook(ctx *PluginContext)
	PreEndHook(ctx *PluginContext)
	PostEndHook(ctx *PluginContext)
}
