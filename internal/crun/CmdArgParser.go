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
	"github.com/spf13/cobra"
	"os"
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
		Use:   "crun",
		Short: "allocate resource and create terminal",
		Run: func(cmd *cobra.Command, args []string) {
			Crun(cmd, args)
		},
	}

	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	rootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "D",
		"info", "Output level")
	rootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 0, " number of nodes on which to run (N = min[-max])")
	rootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 0, "number of cpus required per task")
	rootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 0, "number of tasks to invoke on each node")
	rootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "time limit")
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
