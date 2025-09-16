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

	CreateCmd = &cobra.Command{
		Use:    "create is not supported",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   createExecute,
		Hidden: true,
	}

	StartCmd = &cobra.Command{
		Use:    "start is not supported",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   startExecute,
		Hidden: true,
	}

	RestartCmd = &cobra.Command{
		Use:    "restart is not supported",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   restartExecute,
		Hidden: true,
	}
)

var (
	// Container runtime flags (Docker-compatible)
	FlagName       string
	FlagPorts      []string
	FlagEnv        []string
	FlagVolume     []string
	FlagDetach     bool
	FlagEntrypoint string
	FlagUser       string
	FlagUserNS     bool
	FlagWorkdir    string

	// Resource control flags (Docker-style, mapped to cbatch semantics)
	FlagCpus   float64
	FlagMemory string // Docker-style, deprecated in favor of --mem
	FlagGpus   string // Docker-style, deprecated in favor of --gres

	// cbatch-compatible resource flags (preferred)
	FlagMem  string
	FlagGres string

	// Cluster scheduling flags (full names, avoid Docker shortcuts)
	FlagPartition     string
	FlagNodelist      string
	FlagTime          string
	FlagAccount       string
	FlagQos           string
	FlagNodes         uint32
	FlagNtasksPerNode uint32
	FlagExclusive     bool
	FlagHold          bool
	FlagReservation   string
	FlagExcludes      string
	FlagExtraAttr     string
	FlagMailType      string
	FlagMailUser      string
	FlagComment       string

	// Stop flags
	FlagTimeout int

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

	// Container runtime flags (Docker-compatible)
	RunCmd.Flags().StringVar(&FlagName, "name", "", "Assign a name to the container")
	RunCmd.Flags().StringSliceVarP(&FlagPorts, "ports", "p", []string{}, "Publish a container's port(s) to the host")
	RunCmd.Flags().StringSliceVarP(&FlagEnv, "env", "e", []string{}, "Set environment variables")
	RunCmd.Flags().StringSliceVarP(&FlagVolume, "volume", "v", []string{}, "Bind mount a volume")
	RunCmd.Flags().BoolVarP(&FlagDetach, "detach", "d", false, "Run container in background and print container ID")
	RunCmd.Flags().StringVar(&FlagEntrypoint, "entrypoint", "", "Override the default entrypoint of the image")
	RunCmd.Flags().StringVarP(&FlagUser, "user", "u", "", "Username or UID (format: <name|uid>[:<group|gid>]). With --userns=false, only current user and accessible groups are allowed")
	RunCmd.Flags().BoolVar(&FlagUserNS, "userns", true, "Enable user namespace (default user becomes the faked root, enabled by default)")
	RunCmd.Flags().StringVarP(&FlagWorkdir, "workdir", "w", "", "Working directory inside the container")

	// Resource control flags (Docker-style, compatibility)
	RunCmd.Flags().Float64Var(&FlagCpus, "cpus", 0, "Number of CPUs (maps to cpus-per-task)")
	RunCmd.Flags().StringVar(&FlagMemory, "memory", "", "Memory limit (e.g., 2g, 512m) - deprecated, use --mem")
	RunCmd.Flags().StringVar(&FlagGpus, "gpus", "", "GPU devices - deprecated, use --gres instead")

	// cbatch-compatible resource options (preferred)
	RunCmd.Flags().StringVar(&FlagMem, "mem", "", "Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RunCmd.Flags().StringVar(&FlagGres, "gres", "", "Gres required per task, format: \"gpu:a100:1\" or \"gpu:1\"")

	// Cluster scheduling flags (full names, avoid Docker shortcuts)
	RunCmd.Flags().StringVar(&FlagPartition, "partition", "", "Partition for job scheduling")
	RunCmd.Flags().StringVar(&FlagNodelist, "nodelist", "", "Nodes to be allocated to the job (commas separated list)")
	RunCmd.Flags().StringVar(&FlagTime, "time", "", "Time limit, format: \"day-hours:minutes:seconds\" or \"hours:minutes:seconds\"")
	RunCmd.Flags().StringVar(&FlagAccount, "account", "", "Account used for the job")
	RunCmd.Flags().StringVar(&FlagQos, "qos", "", "QoS used for the job")
	RunCmd.Flags().Uint32Var(&FlagNodes, "nodes", 1, "Number of nodes on which to run")
	RunCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "Number of tasks to invoke on each node")
	RunCmd.Flags().StringVar(&FlagExcludes, "exclude", "", "Exclude specific nodes from allocating (commas separated list)")
	RunCmd.Flags().StringVar(&FlagReservation, "reservation", "", "Use reserved resources")
	RunCmd.Flags().BoolVar(&FlagExclusive, "exclusive", false, "Exclusive node resources")
	RunCmd.Flags().BoolVar(&FlagHold, "hold", false, "Hold the job until it is released")

	// Other task flags
	RunCmd.Flags().StringVar(&FlagExtraAttr, "extra-attr", "", "Extra attributes of the job (in JSON format)")
	RunCmd.Flags().StringVar(&FlagMailType, "mail-type", "", "Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, TIMELIMIT, ALL")
	RunCmd.Flags().StringVar(&FlagMailUser, "mail-user", "", "Mail address of the notification receiver")
	RunCmd.Flags().StringVar(&FlagComment, "comment", "", "Comment of the job")

	// Configure RunCmd to not allow flags after positional arguments
	RunCmd.Flags().SetInterspersed(false)

	// Stop command flags
	StopCmd.Flags().IntVarP(&FlagTimeout, "timeout", "t", 10, "Seconds to wait for stop before killing it")

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
	RootCmd.AddCommand(RunCmd, StopCmd, RmCmd, PsCmd, InspectCmd, LogCmd, LoginCmd, LogoutCmd)
	RootCmd.AddCommand(CreateCmd, StartCmd, RestartCmd) // Hidden commands for compatibility
}
