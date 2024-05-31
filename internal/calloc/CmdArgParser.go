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

package calloc

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

	FlagConfigFilePath string
	FlagDebugLevel     string

	RootCmd = &cobra.Command{
		Use:   "calloc",
		Short: "allocate resource and create terminal",
		Run: func(cmd *cobra.Command, args []string) {
			main(cmd, args)
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorExecuteFailed)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "D",
		"info", "Output level")
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 1, " number of nodes on which to run (N = min[-max])")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "number of cpus required per task")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "number of tasks to invoke on each node")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "time limit")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "minimum amount of real memory")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "account used by the task")
	RootCmd.Flags().StringVar(&FlagCwd, "chdir", "", "working directory of the task")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "quality of service")
}
