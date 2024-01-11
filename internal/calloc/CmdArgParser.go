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
	FlagGres          string

	FlagConfigFilePath string
	FlagDebugLevel     string
)

func CmdArgParser() *cobra.Command {
	parser := &cobra.Command{
		Use:   "calloc",
		Short: "allocate resource and create terminal",
		Run: func(cmd *cobra.Command, args []string) {
			main(cmd, args)
		},
	}

	parser.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	parser.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "D",
		"info", "Output level")
	parser.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 1, " number of nodes on which to run (N = min[-max])")
	parser.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "number of cpus required per task")
	parser.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "number of tasks to invoke on each node")
	parser.Flags().StringVarP(&FlagTime, "time", "t", "", "time limit")
	parser.Flags().StringVar(&FlagMem, "mem", "", "minimum amount of real memory")
	parser.Flags().StringVarP(&FlagPartition, "partition", "p", "", "partition requested")
	parser.Flags().StringVarP(&FlagJob, "job-name", "J", "", "name of job")
	parser.Flags().StringVarP(&FlagAccount, "account", "A", "", "account used by the task")
	parser.Flags().StringVar(&FlagCwd, "chdir", "", "working directory of the task")
	parser.Flags().StringVarP(&FlagQos, "qos", "q", "", "quality of service")
	parser.Flags().StringVar(&FlagGres, "gres", "", "name,type,num of gres required per task")
	return parser
}
