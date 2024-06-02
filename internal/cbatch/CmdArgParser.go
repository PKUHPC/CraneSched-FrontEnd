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
	FlagGetUserEnv    bool
	FlagExport        string
	FlagStdoutPath    string
	FlagStderrPath    string

	FlagConfigFilePath string

	FlagMailType string
	FlagMailUser string

	RootCmd = &cobra.Command{
		Use:   "cbatch",
		Short: "submit batch jobs",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := Cbatch(args[0]); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorExecuteFailed)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 0, " number of nodes on which to run (N = min[-max])")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 0, "number of cpus required per task")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 0, "number of tasks to invoke on each node")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "time limit")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "minimum amount of real memory, default unit is Byte(B), support GB(G, g), MB(M, m), KB(K, k)")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "account used by the task")
	RootCmd.Flags().StringVar(&FlagCwd, "chdir", "", "working directory of the task")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "quality of service")
	RootCmd.Flags().Uint32Var(&FlagRepeat, "repeat", 1, "submit the task multiple times")
	RootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "List of specific nodes to be allocated to the job, separated by commas")
	RootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "exclude a specific list of hosts, separated by commas")
	RootCmd.Flags().BoolVar(&FlagGetUserEnv, "get-user-env", false, "load login environment variables")
	RootCmd.Flags().StringVar(&FlagExport, "export", "", "propagate environment variables")
	RootCmd.Flags().StringVarP(&FlagStdoutPath, "output", "o", "", "file for batch script's standard output")
	RootCmd.Flags().StringVarP(&FlagStderrPath, "error", "e", "", "file for batch script's standard error output")
	RootCmd.Flags().StringVar(&FlagMailType, "mail-type", "", "notify user by mail when certain events occur")
	RootCmd.Flags().StringVar(&FlagMailUser, "mail-user", "", "mail address of the notification receiver")
}
