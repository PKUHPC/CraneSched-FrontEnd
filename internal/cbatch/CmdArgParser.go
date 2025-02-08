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

package cbatch

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
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
	FlagGres          string
	FlagGetUserEnv    bool
	FlagExport        string
	FlagStdoutPath    string
	FlagStderrPath    string

	FlagWrappedScript string

	FlagExtraAttr string
	FlagMailType  string
	FlagMailUser  string

	FlagConfigFilePath string
	FlagJson           bool

	RootCmd = &cobra.Command{
		Use:     "cbatch [flags] file",
		Short:   "Submit batch job",
		Version: util.Version(),
		Args: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("wrap") {
				if len(args) != 0 {
					return errors.New("--wrap is exclusive with file name argument")
				}
			} else if len(args) != 1 {
				return errors.New("invalid number of arguments")
			}
			return nil
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		Run: func(cmd *cobra.Command, args []string) {
			if FlagRepeat == 0 {
				log.Error("Invalid argument: --repeat must > 0.")
				os.Exit(util.ErrorCmdArg)
			}

			cbatchArgs := make([]CbatchArg, 0)
			shScript := ""

			if FlagWrappedScript == "" {
				shLines := make([]string, 0)
				if err := ParseCbatchScript(args[0], &cbatchArgs, &shLines); err != util.ErrorSuccess {
					os.Exit(err)
				}
				shScript = strings.Join(shLines, "\n")
			} else {
				shScript = FlagWrappedScript
			}

			ok, task := ProcessCbatchArgs(cmd, cbatchArgs)
			if !ok {
				os.Exit(util.ErrorCmdArg)
			}

			task.GetBatchMeta().ShScript = shScript
			task.Uid = uint32(os.Getuid())
			task.Gid = uint32(os.Getgid())
			task.CmdLine = strings.Join(os.Args, " ")

			// Process the content of --get-user-env
			util.SetPropagatedEnviron(task)

			task.Type = protos.TaskType_Batch
			if task.Cwd == "" {
				task.Cwd, _ = os.Getwd()
			}

			var err util.CraneCmdError
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
	RootCmd.Flags().StringVar(&FlagGres, "gres", "", "Gres required per task,format: \"gpu:a100:1\" or \"gpu:1\"")
	RootCmd.Flags().Uint32Var(&FlagNtasksPerNode, "ntasks-per-node", 1, "Number of tasks to invoke on each node")
	RootCmd.Flags().StringVarP(&FlagTime, "time", "t", "", "Time limit, format: \"day-hours:minutes:seconds\" 5-0:0:1 for 5 days, 1 second or \"hours:minutes:seconds\" 10:1:2 for 10 hours, 1 minute, 2 seconds")
	RootCmd.Flags().StringVar(&FlagMem, "mem", "", "Maximum amount of real memory, support GB(G, g), MB(M, m), KB(K, k) and Bytes(B), default unit is MB")
	RootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Partition requested")
	RootCmd.Flags().StringVarP(&FlagJob, "job-name", "J", "", "Name of job")
	RootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "", "Account used for the job")
	RootCmd.Flags().StringVarP(&FlagCwd, "chdir", "D", "", "Working directory of the job")
	RootCmd.Flags().StringVarP(&FlagQos, "qos", "q", "", "QoS used for the job")
	RootCmd.Flags().Uint32Var(&FlagRepeat, "repeat", 1, "Submit the job multiple times")
	RootCmd.Flags().StringVarP(&FlagNodelist, "nodelist", "w", "", "Nodes to be allocated to the job (commas separated list)")
	RootCmd.Flags().StringVarP(&FlagExcludes, "exclude", "x", "", "Exclude specific nodes from allocating (commas separated list)")
	RootCmd.Flags().BoolVar(&FlagGetUserEnv, "get-user-env", false, "Load login environment variables of the user")
	RootCmd.Flags().StringVar(&FlagExport, "export", "", "Propagate environment variables")
	RootCmd.Flags().StringVarP(&FlagStdoutPath, "output", "o", "", "Redirection path of standard output of the script")
	RootCmd.Flags().StringVarP(&FlagStderrPath, "error", "e", "", "Redirection path of standard error of the script")
	RootCmd.Flags().StringVar(&FlagWrappedScript, "wrap", "", "Wrap command string in a sh script and submit")
	RootCmd.Flags().StringVar(&FlagExtraAttr, "extra-attr", "", "Extra attributes of the job (in JSON format)")
	RootCmd.Flags().StringVar(&FlagMailType, "mail-type", "", "Notify user by mail when certain events occur, supported values: NONE, BEGIN, END, FAIL, ALL (default is NONE)")
	RootCmd.Flags().StringVar(&FlagMailUser, "mail-user", "", "Mail address of the notification receiver")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
}
