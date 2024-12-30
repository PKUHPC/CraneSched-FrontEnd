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

package cacct

import (
	"CraneFrontEnd/internal/util"
	"os"

	log "github.com/sirupsen/logrus"
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
	FlagFilterStates     string
	FlagFilterPartitions string
	FlagFilterQos        string
	FlagNumLimit         uint32
	FlagNoHeader         bool
	FlagFull             bool
	FlagJson             bool

	RootCmd = &cobra.Command{
		Use:     "cacct [flags]",
		Short:   "Display the recent job information",
		Version: util.Version(),
		Long:    "",
		Args:    cobra.ExactArgs(0),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("max-lines") {
				if FlagNumLimit == 0 {
					log.Error("Output line number limit must be greater than 0.")
					os.Exit(util.ErrorCmdArg)
				}
			}

			if err := QueryJob(); err != util.ErrorSuccess {
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
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
		"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	RootCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
		"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
	RootCmd.Flags().StringVarP(&FlagFilterSubmitTime, "submit-time", "s",
		"", "Filter jobs with a submit time within a certain time period, which can use closed intervals"+
			"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41) or "+
			"semi open intervals(timeFormat: 2024-01-02T15:04:05~ or ~2024-01-11T11:12:41)")
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
	RootCmd.Flags().StringVarP(&FlagFilterQos, "qos", "q", "",
		"Specify QoS of jobs to view (comma separated list), default is all.")
	RootCmd.Flags().StringVarP(&FlagFilterStates, "state", "t",
		"all", "Specify job states to view, supported states: "+
			"pending(p), running(r), completed(c), failed(f), cancelled(x), time-limit-exceeded(t), all.")
	RootCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
		"Specify partitions to view (comma separated list), default is all")

	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format.
Fields are identified by a percent sign (%) followed by a character. 
Use a dot (.) and a number between % and the format character to specify a minimum width for the field. 

Supported format identifiers:
	%a: Account   - Displays the account associated with the job.
	%c: AllocCPUs - Displays the number of allocated CPUs, formatted to two decimal places.
	%e: ExitCode  - Displays the exit code of the job. 
	                If the exit code is based on a specific base (e.g., kCraneExitCodeBase),
	                it formats as "0:<code>" or "<code>:0" based on the condition.
	%j: JobID     - Displays the ID of the job.
	%n: Name      - Displays the name of the job.
	%P: Partition - Displays the partition associated with the job.
	%t: State     - Displays the state of the job.

Each format specifier can be modified with a width specifier (e.g., "%.5j").
If the width is specified, the field will be formatted to at least that width. 
If the format is invalid or unrecognized, the program will terminate with an error message.

Example: --format "%.5j %.20n %t" would output the job's ID with a minimum width of 5,
         Name with a minimum width of 20, and the State.
`)
	RootCmd.Flags().BoolVarP(&FlagFull, "full", "F", false, "Display full information (If not set, only display 30 characters per cell)")
	RootCmd.Flags().Uint32VarP(&FlagNumLimit, "max-lines", "m", 0,
		"Limit the number of lines in the output, default is 0 (no limit)")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
}
