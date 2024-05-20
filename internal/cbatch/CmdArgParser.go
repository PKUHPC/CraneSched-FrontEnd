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

package cbatch

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
	FlagRepeat        uint32
	FlagNodelist      string
	FlagExcludes      string
	FlagGetUserEnv    string
	FlagExport        string
	FlagContainer     string
	FlagInterpreter   string
	FlagStdoutPath    string
	FlagStderrPath    string

	FlagConfigFilePath string
)

func ParseCmdArgs() {
	rootCmd := &cobra.Command{
		Use:   "cbatch",
		Short: "submit batch jobs",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			Cbatch(args[0])
		},
	}

	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "path to configuration file")
	rootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 0, "number of nodes on which to run (N = min[-max])")
	rootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 0, "number of cpus required per task")
	rootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 0, "number of tasks to invoke on each node")
	rootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "time limit")
	rootCmd.Flags().StringVar(&FlagMem, "mem", "", "minimum amount of real memory")
	rootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "partition requested")
	rootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "name of job")
	rootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "account used by the task")
	rootCmd.Flags().StringVar(&FlagCwd, "chdir", "", "working directory of the task")
	rootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "quality of service")
	rootCmd.Flags().Uint32Var(&FlagRepeat, "repeat", 1, "submit the task multiple times")
	rootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "list of specific nodes to be allocated to the job")
	rootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "exclude a specific list of hosts")
	rootCmd.Flags().StringVar(&FlagGetUserEnv, "get-user-env", "", "get user's environment variables")
	rootCmd.Flags().StringVar(&FlagExport, "export", "", "propagate environment variables")
	rootCmd.Flags().StringVar(&FlagContainer, "container", "", "OCI bundle path of the container")
	rootCmd.Flags().StringVar(&FlagInterpreter, "interpreter", "", "interpreter for batch script")
	rootCmd.Flags().StringVarP(&FlagStdoutPath, "output", "o", "", "file for batch script's standard output")
	rootCmd.Flags().StringVarP(&FlagStderrPath, "error", "e", "", "file for batch script's standard error output")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
