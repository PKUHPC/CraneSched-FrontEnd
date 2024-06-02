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

package cacct

import (
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath   string
	FlagFormat           string
	FlagFilterSubmitTime string
	FlagFilterStartTime  string
	FlagFilterEndTime    string
	FlagFilterAccounts   string
	FlagFilterJobIDs     string
	FlagFilterUsers      string
	FlagFilterJobNames   string
	FlagNoHeader         bool
	FlagNumLimit         int32

	RootCmd = &cobra.Command{
		Use:   "cacct",
		Short: "display the recent job information for all queues in the cluster",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := QueryJob(); err != util.ErrorSuccess {
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
	RootCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
		"", "Select jobs eligible before this time")
	RootCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
		"", "Select jobs eligible after this time")
	RootCmd.Flags().StringVarP(&FlagFilterSubmitTime, "submit-time", "s",
		"", "Select jobs eligible after this time")
	RootCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
		"Select accounts to view (comma separated list)")
	RootCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
		"Select users to view (comma separated list)")
	RootCmd.Flags().StringVarP(&FlagFilterJobIDs, "job", "j", "",
		"Select job ids to view (comma separated list), default is all")
	RootCmd.Flags().StringVarP(&FlagFilterJobNames, "name", "n", "",
		"Select job names to view (comma separated list), default is all")
	RootCmd.Flags().BoolVarP(&FlagNoHeader, "noheader", "N", false,
		"Do not print header line in the output")

	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format for the command. 
Fields are identified by a percent sign (%) followed by a character. 
Use a dot (.) and a number between % and the format character to specify a minimum width for the field. 

Supported format identifiers:
	%j: JobId     - Displays the ID of the job. Optionally, use %.<width>j to specify a fixed width.
	%n: JobName   - Displays the name of the job.
	%P: Partition - Displays the partition associated with the job.
	%a: Account   - Displays the account associated with the job.
	%c: AllocCPUs - Displays the number of allocated CPUs, formatted to two decimal places.
	%t: State     - Displays the state of the job.
	%e: ExitCode  - Displays the exit code of the job. 
                    If the exit code is based on a specific base (e.g., kCraneExitCodeBase),
                    it formats as "0:<code>" or "<code>:0" based on the condition.
		
Example:
--format "%j %.10n %P %a %.2c %t %e" would output
the job’s ID, Name with a minimum width of 10, Partition, Account, 
Allocated CPUs with two decimal places, State, and Exit Code.

Each part of the format string controls the appearance of the corresponding column in the output table. 
If the width is specified, the field will be formatted to at least that width. 
If the format is invalid or unrecognized, the program will terminate with an error message.
`)

	RootCmd.Flags().Int32VarP(&FlagNumLimit, "max-lines", "m", 0,
		"Limit the number of lines in the output, default is 0 (no limit)")
}
