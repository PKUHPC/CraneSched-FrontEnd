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
)

func CmdArgParser() {
	rootCmd := &cobra.Command{
		Use:     "crun [flags] executable",
		Short:   "Allocate resource and run executable interactive",
		Version: util.Version(),
		Run: func(cmd *cobra.Command, args []string) {
			MainCrun(cmd, args)
		},
	}

	rootCmd.SetVersionTemplate(util.VersionTemplate())
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	rootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "D",
		"info", "Available debug level: trace,debug,info")
	rootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 1, "number of nodes on which to run (N = min[-max])")
	rootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "number of cpus required per task")
	rootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "number of tasks to invoke on each node")
	rootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "Time limit, format: \"day-hours:minutes:seconds\" 5-0:0:1 for 5 days, 1 second or \"hours:minutes:seconds\" 10:1:2 for 10 hours, 1 minute, 2 seconds")
	rootCmd.Flags().StringVar(&FlagMem, "mem", "", "minimum amount of real memory")
	rootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "partition requested")
	rootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "name of job")
	rootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "account used by the task")
	rootCmd.Flags().StringVar(&FlagCwd, "chdir", "", "working directory of the task")
	rootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "quality of service")
	rootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "List of specific nodes to be allocated to the job")
	rootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "exclude a specific list of hosts")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
