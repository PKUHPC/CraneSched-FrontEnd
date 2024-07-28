package api

// A plugin is a shared object that implements the Plugin interface
type Plugin interface {
	// Init the plugin's basic information
	Init() error

	PreRunHook()
	PostRunHook()
	PreCompletionHook()
	PostCompletionHook()
}
