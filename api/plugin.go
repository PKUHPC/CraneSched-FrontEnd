package api

type HookType uint8

const (
	StartHook HookType = iota
	EndHook
)

type PluginHandler func(*PluginContext)

type PluginMeta struct {
	Name   string `yaml:"Name"`
	Path   string `yaml:"Path"`
	Config string `yaml:"Config"`
}

// A plugin is a shared object that implements the Plugin interface
type Plugin interface {
	// Init the plugin
	Init(meta PluginMeta) error

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
	StartHook(ctx *PluginContext)
	EndHook(ctx *PluginContext)
}
