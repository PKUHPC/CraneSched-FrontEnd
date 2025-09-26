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
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagCraneConfig string
	FlagDebugLevel  string
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
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Failed to parse CraneSched config",
			}
		}

		// Parse plugin part in the config
		if err := ParsePluginConfig(config.CraneBaseDir, FlagCraneConfig); err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the plugin part in config: %s", err),
			}
		}

		if !gPluginConfig.Enabled {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Plugind is disabled in config.",
			}
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
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to load plugins: %s", err),
			}
		}

		// Init plugins
		log.Info("Initializing plugins...")
		for _, p := range gPluginMap {
			if err := (*p).Load(p.Meta); err != nil {
				return &util.CraneError{
					Code:    util.ErrorGeneric,
					Message: fmt.Sprintf("Failed to init plugin: %s", err),
				}
			}
		}

		// Initialize task query service
		log.Info("Initializing task query service...")
		if err := InitTaskQueryService(); err != nil {
			log.Warnf("Failed to initialize task query service: %v", err)
			// Continue without task query service rather than failing completely
		}

		// Create and launch PluginDaemon
		pd := NewPluginD(nil)

		// Start server on UNIX socket
		unixSocket, err := util.GetUnixSocket(gPluginConfig.SockPath, 0600)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorGeneric,
				Message: fmt.Sprintf("Failed to get UNIX socket: %s", err),
			}
		}

		log.Infof("gRPC server listening on %s.", gPluginConfig.SockPath)
		if err := pd.Launch(unixSocket); err != nil {
			return &util.CraneError{
				Code:    util.ErrorGeneric,
				Message: fmt.Sprintf("Failed to launch plugin daemon: %s", err),
			}
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
			return &util.CraneError{
				Code:    util.ErrorGeneric,
				Message: fmt.Sprintf("Failed to unload plugins: %s", err),
			}
		}
		return nil
	},
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.Flags().StringVarP(&FlagCraneConfig, "config", "c", util.DefaultConfigPath, "Path to config file")
	RootCmd.Flags().StringVarP(&FlagDebugLevel, "debug-level", "", "", "Available debug level (trace, debug, info)")
}

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}
