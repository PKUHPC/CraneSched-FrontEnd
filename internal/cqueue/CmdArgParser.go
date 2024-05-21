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
	"github.com/spf13/cobra"
	"os"
)

var (
	FlagConfigFilePath   string
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
	FlagNumLimit         int32

	RootCmd = &cobra.Command{
		Use:   "cqueue",
		Short: "display the job information for all queues in the cluster",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if FlagIterate != 0 {
				loopedQuery(FlagIterate)
			} else {
				Query()
			}
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().BoolVarP(&FlagNoHeader, "noHeader", "N", false,
		"no headers on output")
	RootCmd.Flags().BoolVarP(&FlagStartTime, "start", "S", false,
		"print expected start times of pending jobs")
	RootCmd.Flags().StringVarP(&FlagFilterJobIDs, "job", "j", "",
		"comma separated list of jobs IDs\nto view, default is all")
	RootCmd.Flags().StringVarP(&FlagFilterJobNames, "name", "n", "",
		"comma separated list of job names to view")
	RootCmd.Flags().StringVarP(&FlagFilterQos, "qos", "q", "",
		"comma separated list of qos's\nto view, default is all qos's")
	RootCmd.Flags().StringVarP(&FlagFilterStates, "state", "t", "",
		"comma separated list of states to view,\n"+
			"default is pending and running, \n"+
			"'--states=all' reports all states ")
	RootCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
		"comma separated list of users to view")
	RootCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
		"comma separated list of accounts\n"+
			"to view, default is all accounts")
	RootCmd.Flags().Uint64VarP(&FlagIterate, "iterate", "i", 0,
		"specify an interval in seconds")
	RootCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
		"comma separated list of partitions\n"+
			"to view, default is all partitions")

	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format for the command. 
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

	RootCmd.Flags().Int32VarP(&FlagNumLimit, "MaxVisibleLines", "m", 0,
		"print job information for the specified number of lines")
}
