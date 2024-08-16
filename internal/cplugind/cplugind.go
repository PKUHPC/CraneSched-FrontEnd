/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

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
	FlagCraneConfig string
	FlagDebugLevel  string
)

var RootCmd = &cobra.Command{
	Use:     "cplugind",
	Short:   "cplugind is a plugin daemon for CraneSched",
	Args:    cobra.ExactArgs(0),
	Version: util.Version(),
	Run: func(cmd *cobra.Command, args []string) {
		// Check proxy
		util.DetectNetworkProxy()

		// Parse config
		config := util.ParseConfig(FlagCraneConfig)
		if config == nil {
			log.Errorf("Failed to parse CraneSched config")
			os.Exit(util.ErrorCmdArg)
		}

		// Parse plugin part in the config
		if err := ParsePluginConfig(config.CraneBaseDir, FlagCraneConfig); err != nil {
			log.Errorf("Failed to parse the plugin part in config: %v", err)
			os.Exit(util.ErrorCmdArg)
		}

		if !gPluginConfig.Enabled {
			log.Errorf("Plugind is disabled in config.")
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
		log.Info("Initializing plugins...")
		for _, p := range gPluginMap {
			if err := (*p).Init(p.Meta); err != nil {
				log.Errorf("Failed to init plugin: %v", err)
				os.Exit(util.ErrorGeneric)
			}
		}

		// Create and launch PluginDaemon
		pd := NewPluginD(nil)

		// Start server on UNIX socket
		unixSocket, err := util.GetUnixSocket(gPluginConfig.SockPath, 0600)
		if err != nil {
			log.Errorf("Failed to get UNIX socket: %v", err)
			os.Exit(util.ErrorGeneric)
		}

		log.Infof("gRPC server listening on %s.", gPluginConfig.SockPath)
		if err := pd.Launch(unixSocket); err != nil {
			log.Errorf("Failed to launch plugin daemon: %v", err)
			os.Exit(util.ErrorGeneric)
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
	RootCmd.Flags().StringVarP(&FlagCraneConfig, "config", "c", util.DefaultConfigPath, "Path to config file")
	RootCmd.Flags().StringVarP(&FlagDebugLevel, "debug-level", "", "", "Available debug level (trace, debug, info)")
}

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}
