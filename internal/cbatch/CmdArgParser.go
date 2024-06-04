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
		Use:   "cbatch [flags] file",
		Short: "Submit batch job",
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
		os.Exit(util.ErrorGeneric)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 0, "Number of nodes on which to run (N = min[-max])")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 0, "Number of cpus required per job")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 0, "Number of tasks to invoke on each node")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "Time limit")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "Minimum amount of real memory, default unit is Byte(B), support GB(G, g), MB(M, m), KB(K, k)")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "Name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "Account used for the job")
	RootCmd.Flags().StringVar(&FlagCwd, "chdir", "", "Working directory of the job")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "QoS used for the job")
	RootCmd.Flags().Uint32Var(&FlagRepeat, "repeat", 1, "Submit the job multiple times")
	RootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "Nodes to be allocated to the job (commas separated list)")
	RootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "Exclude specific nodes from allocating (commas separated list)")
	RootCmd.Flags().BoolVar(&FlagGetUserEnv, "get-user-env", false, "Load login environment variables of the user")
	RootCmd.Flags().StringVar(&FlagExport, "export", "", "Propagate environment variables")
	RootCmd.Flags().StringVarP(&FlagStdoutPath, "output", "o", "", "Redirection path of standard output of the script")
	RootCmd.Flags().StringVarP(&FlagStderrPath, "error", "e", "", "Redirection path of standard error of the script")
	RootCmd.Flags().StringVar(&FlagMailType, "mail-type", "", "Notify user by mail when certain events occur")
	RootCmd.Flags().StringVar(&FlagMailUser, "mail-user", "", "Mail address of the notification receiver")
}
