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

package cqueue

import (
	"CraneFrontEnd/internal/util"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath   string
	FlagFull             bool
	FlagNoHeader         bool
	FlagStartTime        bool
	FlagFilterPartitions string
	FlagFilterJobIDs     string
	FlagFilterJobNames   string
	FlagFilterQos        string
	FlagFilterStates     string
	FlagFilterUsers      string
	FlagFilterAccounts   string
	FlagFormat           string
	FlagIterate          uint64
	FlagNumLimit         uint32

	RootCmd = &cobra.Command{
		Use:   "cqueue [flags]",
		Short: "Display the job information and queue status",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("max-lines") {
				if FlagNumLimit == 0 {
					log.Error("Output line number limit must be greater than 0.")
					os.Exit(util.ErrorCmdArg)
				}
			}

			err := util.ErrorSuccess
			if FlagIterate != 0 {
				err = loopedQuery(FlagIterate)
			} else {
				err = Query()
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
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")

	RootCmd.Flags().StringVarP(&FlagFilterJobIDs, "job", "j", "",
		"Specify job ids to view (comma separated list), default is all")
	RootCmd.Flags().StringVarP(&FlagFilterJobNames, "name", "n", "",
		"Specify job names to view (comma separated list), default is all")
	RootCmd.Flags().StringVarP(&FlagFilterQos, "qos", "q", "",
		"Specify QoS of jobs to view (comma separated list), \ndefault is all QoS")
	RootCmd.Flags().StringVarP(&FlagFilterStates, "state", "t", "all",
		"Specify job states to view. Valid value are 'pending(p)', 'running(r)' and 'all'.\n"+
			"By default, 'all' is specified and all pending and running jobs will be reported")
	RootCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
		"Specify users to view (comma separated list), default is all users")
	RootCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
		"Specify accounts to view (comma separated list), \ndefault is all accounts")
	RootCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
		"Specify partitions to view (comma separated list), \ndefault is all partitions")

	RootCmd.Flags().Uint64VarP(&FlagIterate, "iterate", "i", 0,
		"Display at specified intervals (seconds), default is 0 (no iteration)")
	RootCmd.Flags().BoolVarP(&FlagStartTime, "start", "S", false,
		"Display expected start time of pending jobs")
	RootCmd.Flags().BoolVarP(&FlagNoHeader, "noheader", "N", false,
		"Do not print header line in the output")

	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format. 
Fields are identified by a percent sign (%) followed by a character. 
Use a dot (.) and a number between % and the format character to specify a minimum width for the field. 

Supported format identifiers:
    %j: JobID     - Display the job ID of the task. Use "%.5j" for a width of 5.
    %n: Name      - Display the name of the task.
    %t: Status    - Display the current status of the task.
    %P: Partition - Display the partition the task is running in.
    %p: Priority  - Display the task's priority.
    %u: User      - Display the user who submitted the task.
    %a: Account   - Display the account associated with the task.
    %T: Type      - Display the task type.
    %I: NodeList  - Display the list of nodes the task is running on.
    %l: TimeLimit - Display the time limit for the task.
    %N: Nodes     - Display the number of nodes assigned to the task.
    %s: SubmitTime- Display the submission time of the task.
    %q: QoS       - Display the Quality of Service level for the task.

Each format specifier can be modified with a width specifier (e.g., "%.5j").
Example: --format "%.5j %.20n %t" will output tasks' JobID with a minimum width of 5,
         Name with a minimum width of 20, and Status.
`)
	RootCmd.Flags().BoolVarP(&FlagFull, "full", "F", false, "Display full information (If not set, only display 30 characters per cell)")
	RootCmd.Flags().Uint32VarP(&FlagNumLimit, "max-lines", "m", 0,
		"Limit the number of lines in the output, default is 0 (no limit)")
}
