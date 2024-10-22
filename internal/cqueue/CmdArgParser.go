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
	FlagSelf             bool
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
	FlagJson             bool

	RootCmd = &cobra.Command{
		Use:     "cqueue [flags]",
		Short:   "Display the job information and queue status",
		Long:    "",
		Version: util.Version(),
		Args:    cobra.ExactArgs(0),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
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
	RootCmd.SetVersionTemplate(util.VersionTemplate())
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
	RootCmd.Flags().BoolVar(&FlagSelf, "self", false, "Display only the jobs submitted by current user")

	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format. 
Fields are identified by a percent sign (%) followed by a character. 
Use a dot (.) and a number between % and the format character to specify a minimum width for the field. 

Supported format identifiers:
	%a: Account    - Display the account associated with the job.
	%c: CpuPerNode - Display the requested cpu per node of the job.
	%C: AllocCpus  - Display the cpus allocated to the job.
	%D: NodeNum    - Display the number of nodes requested by the job.
	%e: Time       - Display the elapsed time from the start of the job. 
	%j: JobID      - Display the ID of the job.
	%l: TimeLimit  - Display the time limit for the job.
	%L: NodeList   - Display the list of nodes the job is running on.
	%m: MemPerNode - Display the requested mem per node of the job.
	%n: Name       - Display the name of the job.
	%N: Nodes      - Display the number of nodes assigned to the job.
	%p: Priority   - Display the priority of the job.
	%P: Partition  - Display the partition the job is running in.
	%q: QoS        - Display the Quality of Service level for the job.
	%r: Reason     - Display the reason of pending.
	%s: SubmitTime - Display the submission time of the job.
	%t: State      - Display the current state of the job.
	%T: Type       - Display the job type.
	%u: User       - Display the user who submitted the job.
	 

Each format specifier can be modified with a width specifier (e.g., "%.5j").
If the width is specified, the field will be formatted to at least that width. 
If the format is invalid or unrecognized, the program will terminate with an error message.

Example: --format "%.5j %.20n %t" would output Jobs' ID with a minimum width of 5,
         Name with a minimum width of 20, and the State.
`)
	RootCmd.Flags().BoolVarP(&FlagFull, "full", "F", false, "Display full information (If not set, only display 30 characters per cell)")
	RootCmd.Flags().Uint32VarP(&FlagNumLimit, "max-lines", "m", 0,
		"Limit the number of lines in the output, default is 0 (no limit)")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
}
