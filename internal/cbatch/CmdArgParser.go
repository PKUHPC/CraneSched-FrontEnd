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
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
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
		Use:     "cbatch [flags] file",
		Short:   "Submit batch job",
		Version: util.Version(),
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if FlagRepeat == 0 {
				log.Error("--repeat must > 0.")
				os.Exit(util.ErrorCmdArg)
			}

			pArgs := make([]CbatchArg, 0)
			pSh := make([]string, 0)
			if err := ParseCbatchScript(args[0], &pArgs, &pSh); err != util.ErrorSuccess {
				os.Exit(err)
			}

			ok, task := ProcessCbatchArgs(cmd, pArgs)
			if !ok {
				os.Exit(util.ErrorCmdArg)
			}

			task.GetBatchMeta().ShScript = strings.Join(pSh, "\n")
			task.Uid = uint32(os.Getuid())
			task.CmdLine = strings.Join(os.Args, " ")

			// Process the content of --get-user-env
			SetPropagatedEnviron(task)

			task.Type = protos.TaskType_Batch
			if task.Cwd == "" {
				task.Cwd, _ = os.Getwd()
			}

			var err int
			if FlagRepeat == 1 {
				err = SendRequest(task)
			} else {
				err = SendMultipleRequests(task, FlagRepeat)
			}
			os.Exit(err)
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().Uint32VarP(&FlagNodes, "nodes", "N", 1, "Number of nodes on which to run (N = min[-max])")
	RootCmd.Flags().Float64VarP(&FlagCpuPerTask, "cpus-per-task", "c", 1, "Number of cpus required per job")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "Number of tasks to invoke on each node")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "Time limit, format: \"day-hours:minutes:seconds\" 5-0:0:1 for 5 days, 1 second or \"hours:minutes:seconds\" 10:1:2 for 10 hours, 1 minute, 2 seconds")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "Minimum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "Name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "Account used for the job")
	RootCmd.Flags().StringVar(&FlagCwd, "chdir", "D", "Working directory of the job")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "QoS used for the job")
	RootCmd.Flags().Uint32Var(&FlagRepeat, "repeat", 1, "Submit the job multiple times")
	RootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "Nodes to be allocated to the job (commas separated list)")
	RootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "Exclude specific nodes from allocating (commas separated list)")
	RootCmd.Flags().BoolVar(&FlagGetUserEnv, "get-user-env", false, "Load login environment variables of the user")
	RootCmd.Flags().StringVar(&FlagExport, "export", "", "Propagate environment variables")
	RootCmd.Flags().StringVarP(&FlagStdoutPath, "output", "o", "", "Redirection path of standard output of the script")
	RootCmd.Flags().StringVarP(&FlagStderrPath, "error", "e", "", "Redirection path of standard error of the script")
	RootCmd.Flags().StringVar(&FlagMailType, "mail-type", "", "Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, ALL (default is NONE)")
	RootCmd.Flags().StringVar(&FlagMailUser, "mail-user", "", "Mail address of the notification receiver")
}
