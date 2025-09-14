/**
 * Copyright (c) 2025 Peking University and Peking University
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

package ccon

import (
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagJson           bool

	RootCmd = &cobra.Command{
		Use:     "ccon",
		Short:   "Container management tool",
		Version: util.Version(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
	}

	RunCmd = &cobra.Command{
		Use:   "run [flags] IMAGE [COMMAND] [ARG...]",
		Short: "Create and run a new container",
		RunE:  runExecute,
	}

	LogCmd = &cobra.Command{
		Use:   "log [flags] CONTAINER",
		Short: "Fetch the logs of a container",
		RunE:  logExecute,
	}

	LoginCmd = &cobra.Command{
		Use:   "login [flags] SERVER",
		Short: "Log in to a container registry",
		RunE:  loginExecute,
	}

	LogoutCmd = &cobra.Command{
		Use:   "logout SERVER",
		Short: "Log out from a container registry",
		RunE:  logoutExecute,
	}

	StopCmd = &cobra.Command{
		Use:   "stop [flags] CONTAINER",
		Short: "Stop one or more running containers",
		RunE:  stopExecute,
	}

	RmCmd = &cobra.Command{
		Use:    "rm [flags] CONTAINER",
		Short:  "Not supported - containers are auto-cleaned",
		RunE:   rmExecute,
		Hidden: true,
	}

	CreateCmd = &cobra.Command{
		Use:    "create [flags] IMAGE [COMMAND] [ARG...]",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   createExecute,
		Hidden: true,
	}

	StartCmd = &cobra.Command{
		Use:    "start [flags] CONTAINER",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   startExecute,
		Hidden: true,
	}

	RestartCmd = &cobra.Command{
		Use:    "restart [flags] CONTAINER",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   restartExecute,
		Hidden: true,
	}

	PsCmd = &cobra.Command{
		Use:   "ps [flags]",
		Short: "List containers",
		RunE:  psExecute,
	}

	InspectCmd = &cobra.Command{
		Use:   "inspect CONTAINER",
		Short: "Display detailed information on one or more containers",
		RunE:  inspectExecute,
	}
)

var (
	// Run flags
	FlagName       string
	FlagPorts      []string
	FlagEnv        []string
	FlagVolume     []string
	FlagDetach     bool
	FlagEntrypoint string
	FlagUser       string
	FlagUserNS     bool
	FlagWorkdir    string

	// Stop flags
	FlagTimeout int

	// Rm flags
	FlagForce bool

	// Ps flags
	FlagAll   bool
	FlagQuiet bool

	// Log flags
	FlagFollow     bool
	FlagTail       int
	FlagTimestamps bool

	// Login flags
	FlagUsername      string
	FlagPassword      string
	FlagPasswordStdin bool
)

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().BoolVar(&FlagJson, "json", false, "Output in JSON format")

	// Run command flags
	RunCmd.Flags().StringVar(&FlagName, "name", "", "Assign a name to the container")
	RunCmd.Flags().StringSliceVarP(&FlagPorts, "ports", "p", []string{}, "Publish a container's port(s) to the host")
	RunCmd.Flags().StringSliceVarP(&FlagEnv, "env", "e", []string{}, "Set environment variables")
	RunCmd.Flags().StringSliceVarP(&FlagVolume, "volume", "v", []string{}, "Bind mount a volume")
	RunCmd.Flags().BoolVarP(&FlagDetach, "detach", "d", false, "Run container in background and print container ID")
	RunCmd.Flags().StringVar(&FlagEntrypoint, "entrypoint", "", "Override the default entrypoint of the image")
	RunCmd.Flags().StringVarP(&FlagUser, "user", "u", "", "Username or UID (format: <name|uid>[:<group|gid>]). With --userns=false, only current user and accessible groups are allowed")
	RunCmd.Flags().BoolVar(&FlagUserNS, "userns", true, "Enable user namespace (default user becomes the faked root, enabled by default)")
	RunCmd.Flags().StringVarP(&FlagWorkdir, "workdir", "w", "", "Working directory inside the container")

	// Stop command flags
	StopCmd.Flags().IntVarP(&FlagTimeout, "timeout", "t", 10, "Seconds to wait for stop before killing it")

	// Rm command flags
	RmCmd.Flags().BoolVarP(&FlagForce, "force", "f", false, "Force the removal of a running container (uses SIGKILL)")

	// Ps command flags
	PsCmd.Flags().BoolVarP(&FlagAll, "all", "a", false, "Show all containers (default shows just running)")
	PsCmd.Flags().BoolVarP(&FlagQuiet, "quiet", "q", false, "Only display container IDs")

	// Log command flags
	LogCmd.Flags().BoolVarP(&FlagFollow, "follow", "f", false, "Follow log output")
	LogCmd.Flags().IntVar(&FlagTail, "tail", -1, "Number of lines to show from the end of the logs")
	LogCmd.Flags().BoolVarP(&FlagTimestamps, "timestamps", "t", false, "Show timestamps")

	// Login command flags
	LoginCmd.Flags().StringVarP(&FlagUsername, "username", "u", "", "Username for registry authentication")
	LoginCmd.Flags().StringVarP(&FlagPassword, "password", "p", "", "Password for registry authentication")
	LoginCmd.Flags().BoolVar(&FlagPasswordStdin, "password-stdin", false, "Read password from stdin")

	// Add subcommands to root
	RootCmd.AddCommand(RunCmd, StopCmd, RmCmd, CreateCmd, StartCmd, RestartCmd, PsCmd, InspectCmd, LogCmd, LoginCmd, LogoutCmd)
}
