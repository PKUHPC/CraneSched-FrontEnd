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

package crun

import (
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
	RootCmd = &cobra.Command{
		Use:     "crun [flags] executable",
		Short:   "Allocate resource and run executable interactive",
		Version: util.Version(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := MainCrun(cmd, args); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	FlagNodes         uint32
	FlagCpuPerTask    float64
	FlagNtasksPerNode uint32
	FlagTime          string
	FlagMem           string
	FlagPartition     string
	FlagJob           string
	FlagAccount       string
	FlagQos           string
	FlagCwd           string
	FlagNodelist      string
	FlagExcludes      string
	FlagGetUserEnv    bool
	FlagExport        string
	FlagGres          string
	FlagPty           bool
	FlagX11           bool

	FlagConfigFilePath string
	FlagDebugLevel     string
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "",
		"info", "Available debug level: trace, debug, info")
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 1, "Number of nodes on which to run (N = min[-max])")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "Number of cpus required per task")
	RootCmd.Flags().StringVar(&FlagGres, "gres", "", "Gres required per task,format: \"gpu:a100:1\" or \"gpu:1\"")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "Number of tasks to invoke on each node")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "Time limit, format: \"day-hours:minutes:seconds\" 5-0:0:1 for 5 days, 1 second or \"hours:minutes:seconds\" 10:1:2 for 10 hours, 1 minute, 2 seconds")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "Name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "Account used for the job")
	RootCmd.Flags().StringVarP(&FlagCwd, "chdir", "D", "", "Working directory of the job")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "QoS used for the job")
	RootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "Nodes to be allocated to the job (commas separated list)")
	RootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "Exclude specific nodes from allocating (commas separated list)")
	RootCmd.Flags().BoolVar(&FlagGetUserEnv, "get-user-env", false, "Load login environment variables of the user")
	RootCmd.Flags().StringVar(&FlagExport, "export", "", "Propagate environment variables")
	RootCmd.Flags().BoolVar(&FlagPty, "pty", false, "Run with a pseudo-terminal")
	RootCmd.Flags().BoolVar(&FlagX11, "x11", false, "Run with X11 forwarding (insecure)")
}
