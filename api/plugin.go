package api

type HookType uint8

const (
	StartHook HookType = iota
	EndHook
	JobMonitorHook
)

type PluginHandler func(*PluginContext)

type PluginMeta struct {
	Name   string `yaml:"Name"`
	Path   string `yaml:"Path"`
	Config string `yaml:"Config"`
}

// A plugin is a shared object that implements the Plugin interface
type Plugin interface {
	// Init the plugin with the metadata, e.g., read the config file
	Init(meta PluginMeta) error

	// Get the plugin name, should be consistent with the config file
	Name() string

	// Get the plugin version, could be used for simple version control
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
	JobMonitorHook(ctx *PluginContext)
}
