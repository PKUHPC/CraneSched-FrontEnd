/**
 * Copyright (c) 2023 Peking University and Peking University
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

package crun

import (
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
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

	FlagConfigFilePath string
	FlagDebugLevel     string

	RootCmd = &cobra.Command{
		Use:     "crun",
		Short:   "Allocate resource and create terminal",
		Version: util.Version(),
		Run: func(cmd *cobra.Command, args []string) {
			MainCrun(cmd, args)
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "",
		"info", "Available debug level: trace, debug, info")
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 1, "Number of nodes on which to run (N = min[-max])")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "Number of cpus required per task")
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
}
