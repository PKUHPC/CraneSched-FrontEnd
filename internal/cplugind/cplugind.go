package cplugind

import (
	"CraneFrontEnd/internal/util"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagCraneConfig  string
	FlagPluginConfig string
	FlagDebugLevel   string
)

var RootCmd = &cobra.Command{
	Use:     "cplugind",
	Short:   "cplugind is a plugin daemon for CraneSched",
	Args:    cobra.ExactArgs(0),
	Version: util.Version(),
	Run: func(cmd *cobra.Command, args []string) {
		// Parse config
		config := util.ParseConfig(FlagCraneConfig)
		if config == nil {
			log.Errorf("Failed to parse CraneSched config")
			os.Exit(util.ErrorCmdArg)
		}

		// Parse plugin config
		if cmd.Flags().Changed("plugin") {
			log.Tracef("Using plugin config path: %s", FlagPluginConfig)
			config.PluginConfigPath = FlagPluginConfig
		}

		if err := ParsePluginConfig(config.PluginConfigPath); err != nil {
			log.Errorf("Failed to parse plugin config: %v", err)
			os.Exit(util.ErrorCmdArg)
		}

		// Set log level
		if cmd.Flags().Changed("debug-level") {
			util.InitLogger(FlagDebugLevel)
		} else {
			util.InitLogger(gPluginConfig.LogLevel)
		}

		// Load plugins
		log.Info("Loading plugins...")
		if err := LoadPluginsByConfig(gPluginConfig.Plugins); err != nil {
			log.Errorf("Failed to load plugins: %v", err)
			os.Exit(util.ErrorCmdArg)
		}

		// Init plugins
		for _, p := range gPluginList {
			if err := (*p).Init(); err != nil {
				log.Errorf("Failed to init plugin: %v", err)
				os.Exit(util.ErrorGeneric)
			}
		}

		// Create and launch PluginDaemon
		pd := NewPluginD(nil)
		socket, err := util.GetUnixSocket(gPluginConfig.SockPath)
		if err != nil {
			log.Errorf("Failed to get unix socket: %v", err)
			os.Exit(util.ErrorGeneric)
		}

		log.Infof("gRPC server listening on %s.", gPluginConfig.SockPath)
		if err := pd.Launch(socket); err != nil {
			log.Fatalf("Failed to launch plugin daemon: %v", err)
		}

		// Signal handling
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		// Block until a signal is received
		sig := <-sigs
		switch sig {
		case syscall.SIGINT:
			log.Infof("Received SIGINT, exiting...")
			pd.GracefulStop()
		case syscall.SIGTERM:
			log.Infof("Received SIGTERM, exiting...")
			pd.Stop()
		}
	},
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.Flags().StringVarP(&FlagCraneConfig, "config", "c", util.DefaultConfigPath, "Path to CraneSched config file")
	RootCmd.Flags().StringVarP(&FlagPluginConfig, "plugin", "p", "", "Path to cplugind config file (use path in CraneSched config if not set)")
	RootCmd.Flags().StringVarP(&FlagDebugLevel, "debug-level", "", "", "Available debug level (trace, debug, info)")
}

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}
