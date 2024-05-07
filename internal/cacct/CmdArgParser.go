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
	"github.com/spf13/cobra"
	"os"
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

	rootCmd = &cobra.Command{
		Use:   "cacct",
		Short: "display the recent job information for all queues in the cluster",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			Preparation()
		},
		Run: func(cmd *cobra.Command, args []string) {
			QueryJob()
		},
	}
)

func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	rootCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
		"", "Select jobs eligible before this time")
	rootCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
		"", "Select jobs eligible after this time")
	rootCmd.Flags().StringVarP(&FlagFilterSubmitTime, "submit-time", "s",
		"", "Select jobs eligible after this time")
	rootCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
		"comma separated list of accounts\n"+
			"to view, default is all accounts")
	rootCmd.Flags().StringVarP(&FlagFilterJobIDs, "job", "j", "",
		"comma separated list of jobs IDs\nto view, default is all")
	rootCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
		"comma separated list of users to view")
	rootCmd.Flags().StringVarP(&FlagFilterJobNames, "name", "n", "",
		"comma separated list of job names to view")
	rootCmd.Flags().BoolVarP(&FlagNoHeader, "noHeader", "N", false,
		"no headers on output")
	rootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "", `Specify the output format for the command. 
Fields are identified by a percent sign (%) followed by a character. 
Use a dot (.) and a number between % and the format character to specify a minimum width for the field. 

Supported format identifiers:
		%j: JobId     - Displays the ID of the job. Optionally, use %.<width>j to specify a fixed width.
		%n: JobName   - Displays the name of the job.
		%P: Partition - Displays the partition associated with the job.
		%a: Account   - Displays the account associated with the job.
		%c: AllocCPUs - Displays the number of allocated CPUs, formatted to two decimal places.
		%t: State     - Displays the state of the job.
		%e: ExitCode  - Displays the exit code of the job. If the exit code is based on a specific base (e.g., kCraneExitCodeBase), it formats as "0:<code>" or "<code>:0" based on the condition.
		
Example:
--format "%j %.10n %P %a %.2c %t %e" would output the job’s ID, Name with a minimum width of 10, Partition, Account, Allocated CPUs with two decimal places, State, and Exit Code.

Each part of the format string controls the appearance of the corresponding column in the output table. If the width is specified, the field will be formatted to at least that width. If the format is invalid or unrecognized, the program will terminate with an error message.
`)
	rootCmd.Flags().Int32VarP(&FlagNumLimit, "MaxVisibleLines", "m", 0,
		"print job information for the specified number of lines")
}
