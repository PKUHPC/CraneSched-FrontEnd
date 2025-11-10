/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package cplugind

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/internal/util"
	"net"
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
	RunE: func(cmd *cobra.Command, args []string) error {
		// Check proxy
		util.DetectNetworkProxy()

		// Parse config
		config := util.ParseConfig(FlagCraneConfig)
		if config == nil {
			log.Errorf("Failed to parse CraneSched config")
			return &util.CraneError{Code: util.ErrorCmdArg}
		}

		// Parse plugin configuration from separate plugin.yaml file
		if err := ParsePluginConfig(config.CraneBaseDir, FlagPluginConfig); err != nil {
			log.Errorf("Failed to parse plugin config from %s: %s", FlagPluginConfig, err)
			return &util.CraneError{Code: util.ErrorCmdArg}
		}

		if !gPluginConfig.Enabled {
			log.Errorf("Plugind is disabled in config.")
			return &util.CraneError{Code: util.ErrorCmdArg}
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
			log.Errorf("Failed to load plugins: %s", err)
			return &util.CraneError{Code: util.ErrorCmdArg}
		}

		// Provide config path to plugins that require it and initialize them
		log.Info("Initializing plugins...")
		for _, loaded := range gPluginMap {
			pluginImpl := loaded.Plugin

			if aware, ok := pluginImpl.(api.HostConfigAware); ok {
				aware.SetHostConfigPath(FlagCraneConfig)
			}

			if err := pluginImpl.Load(loaded.Meta); err != nil {
				log.Errorf("Failed to init plugin: %s", err)
				return &util.CraneError{Code: util.ErrorGeneric}
			}
		}

		// Create and launch PluginDaemon
		pd := NewPluginD(nil)

		// Allow plugins to register additional gRPC services before serving
		for _, loaded := range gPluginMap {
			pluginImpl := loaded.Plugin

			if registrar, ok := pluginImpl.(api.GrpcServiceRegistrar); ok {
				if err := registrar.RegisterGrpcServices(pd.Server); err != nil {
					log.Errorf("Failed to register gRPC services for plugin %s: %v", loaded.Meta.Name, err)
					return &util.CraneError{Code: util.ErrorGeneric}
				}
			}
		}

		// Prepare listeners based on configuration
		listeners := make([]net.Listener, 0, 2)

		unixSocket, err := util.GetUnixSocket(gPluginConfig.SockPath, 0600)
		if err != nil {
			log.Errorf("Failed to get UNIX socket: %s", err)
			return &util.CraneError{Code: util.ErrorGeneric}
		}
		listeners = append(listeners, unixSocket)
		log.Infof("gRPC server listening on UNIX socket %s.", gPluginConfig.SockPath)

		if addr := gPluginConfig.ListenAddress; addr != "" {
			port := gPluginConfig.ListenPort
			if port == "" {
				log.Errorf("PlugindListenPort must be specified when PlugindListenAddress is set")
				return &util.CraneError{Code: util.ErrorCmdArg}
			}

			bindTarget := net.JoinHostPort(addr, port)
			tcpListener, err := util.GetTCPSocket(bindTarget, config)
			if err != nil {
				log.Errorf("Failed to listen on %s: %v", bindTarget, err)
				return &util.CraneError{Code: util.ErrorGeneric}
			}
			listeners = append(listeners, tcpListener)
			log.Infof("gRPC server also listening on %s.", bindTarget)
		}

		if err := pd.Launch(listeners...); err != nil {
			log.Errorf("Failed to launch plugin daemon: %s", err)
			return &util.CraneError{Code: util.ErrorGeneric}
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

		// After stopping gRPC server, unload plugins
		if err := UnloadPlugins(); err != nil {
			log.Errorf("Failed to unload plugins: %s", err)
			return &util.CraneError{Code: util.ErrorGeneric}
		}
		return nil
	},
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.Flags().StringVarP(&FlagCraneConfig, "config", "c", util.DefaultConfigPath, "Path to config file")
	RootCmd.Flags().StringVarP(&FlagPluginConfig, "plugin-config", "p", util.DefaultPluginConfigPath, "Path to plugin config file")
	RootCmd.Flags().StringVarP(&FlagDebugLevel, "debug-level", "", "", "Available debug level (trace, debug, info)")
}

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}
