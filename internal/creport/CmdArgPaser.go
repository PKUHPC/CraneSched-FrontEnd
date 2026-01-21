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

package creport

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath   string
	FlagFilterStartTime  string
	FlagFilterEndTime    string
	FlagFilterAccounts   string
	FlagFilterUsers      string
	FlagFilterQosList    string
	FlagOutType          string
	FlagGroupSet         bool
	FlagFilterWckeys     string
	FlagFilterGids       string
	FlagFilterGrouping   string
	FlagFilterJobIDs     string
	FlagFilterPartitions string
	FlagFilterNodeNames  string
	FlagTopCount         uint32
	FlagPrintJobCount    bool
	FlagJson             bool

	RootCmd = &cobra.Command{
		Use:     "creport",
		Short:   "Display system jobs info report",
		Long:    "",
		Version: util.Version(),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			return nil
		},
	}

	userCmd = &cobra.Command{
		Use:   "user",
		Short: "Display system user jobs info report",
		Long:  "",
	}

	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Display system cluster jobs info report",
		Long:  "",
	}

	jobCmd = &cobra.Command{
		Use:   "job",
		Short: "Display system job size info report",
		Long:  "",
	}

	userTopUsageCmd = &cobra.Command{
		Use:   "topusage",
		Short: "Display statistical information of the top job accounts under the specified users",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("group") {
				FlagGroupSet = true
			} else {
				FlagGroupSet = false
			}
			if cmd.Flags().Changed("topcount") {
				if FlagTopCount == 0 {
					return util.NewCraneErr(util.ErrorCmdArg, "Output line number limit must be greater than 0")
				}
			}
			return QueryUsersTopSummaryItem()
		},
	}

	accountUtilizationByUserCmd = &cobra.Command{
		Use:   "accountutilizationbyuser",
		Short: "Display statistical information of all job accounts under the specified users",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_USER)
		},
	}
	userUtilizationByAccountCmd = &cobra.Command{
		Use:   "userutilizationbyaccount",
		Short: "Display statistical information for all job users under the specified accounts",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_ACCOUNT)
		},
	}
	userUtilizationByWckeyCmd = &cobra.Command{
		Use:   "userutilizationbywckey",
		Short: "Display the statistical information for all job users under the specified wckeys",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_WCKEY)
		},
	}
	wckeyUtilizationByUserCmd = &cobra.Command{
		Use:   "wckeyutilizationbyuser",
		Short: "Display the statistical information for all job wckeys under the specified users",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_WCKEY_UTILIZATION_BY_USER)
		},
	}
	accountUtilizationByQosCmd = &cobra.Command{
		Use:   "accountutilizationbyqos",
		Short: "Display the statistical information for all job accounts under the specified qos",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_QOS)
		},
	}
	// utilizationCmd = &cobra.Command{
	// 	Use:   "utilization",
	// 	Short: "Display relevant cluster parameters",
	// 	Long:  "",
	// 	Args:  cobra.ExactArgs(0),
	// 	RunE: func(cmd *cobra.Command, args []string) error {
	// 		return fmt.Errorf("Not implemented yet")
	// 	},
	// }
	sizesByAccountCmd = &cobra.Command{
		Use:   "sizesbyaccount",
		Short: "Display job size statistics information under the specified accounts",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSizeSummary(CheckAccountCpusStatus)
		},
	}
	sizesByWckeyCmd = &cobra.Command{
		Use:   "sizesbywckey",
		Short: "Display job size statistics information under the specified wckeys",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSizeSummary(CheckWckeyCpusStatus)
		},
	}
	sizesByAccountAndWcKey = &cobra.Command{
		Use:   "sizesbyaccountandwckey",
		Short: "Display job size statistics information under the specified accounts and wckeys",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSizeSummary(CheckAccountWckeyCpusStatus)
		},
	}
)

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.AddCommand(userCmd)
	{
		userCmd.AddCommand(userTopUsageCmd)
		{
			userTopUsageCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			userTopUsageCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			userTopUsageCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			userTopUsageCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userTopUsageCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes)")
			userTopUsageCmd.Flags().Uint32VarP(&FlagTopCount, "topcount", "", 10, "Change the number of users displayed, default is 10 ")
			userTopUsageCmd.Flags().BoolVar(&FlagGroupSet, "group", false,
				"Group all accounts together for each user, Default is a separate entry for each user and account reference")
			userTopUsageCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}

	}
	RootCmd.AddCommand(clusterCmd)
	{
		clusterCmd.AddCommand(accountUtilizationByUserCmd)
		{
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			accountUtilizationByUserCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
		clusterCmd.AddCommand(userUtilizationByAccountCmd)
		{
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			userUtilizationByAccountCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
		clusterCmd.AddCommand(userUtilizationByWckeyCmd)
		{
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			userUtilizationByWckeyCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
		clusterCmd.AddCommand(wckeyUtilizationByUserCmd)
		{
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
			wckeyUtilizationByUserCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
		clusterCmd.AddCommand(accountUtilizationByQosCmd)
		{
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterQosList, "qos", "q",
				"", "Select QoSs to view (comma separated list)")
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			accountUtilizationByQosCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
		// clusterCmd.AddCommand(utilizationCmd)
		// {
		// 	utilizationCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
		// 		GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
		// 	utilizationCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
		// 		GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
		// 	utilizationCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
		// 		"Set the job output time unit(seconds/minutes/hours, default: minutes")
		// 	utilizationCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		// }

	}
	RootCmd.AddCommand(jobCmd)
	{
		jobCmd.AddCommand(sizesByAccountCmd)
		{
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			sizesByAccountCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterGids, "gid", "", "",
				"Select group id to view (comma separated list)")
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterGrouping, "grouping", "", "50,250,500,1000",
				"Comma separated list of size groupings "+
					"(e.g. 50,100,150 would group job cpu count 1-49, 50-99, 100-149, > 150). "+
					"grouping=individual will result in a single column for each job size found.")
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
				"Specify partitions to view (comma separated list), default is all")
			sizesByAccountCmd.Flags().BoolVarP(&FlagPrintJobCount, "printjobcount", "", false,
				" The report will print number of jobs range instead of time used")
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterNodeNames, "nodes", "n", "",
				"Specify nodes name to view (comma separated list), default is all")
			sizesByAccountCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
		jobCmd.AddCommand(sizesByWckeyCmd)
		{
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			sizesByWckeyCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterGids, "gid", "", "",
				"Select group id to view (comma separated list)")
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterGrouping, "grouping", "", "50,250,500,1000",
				"Comma separated list of size groupings "+
					"(e.g. 50,100,150 would group job cpu count 1-49, 50-99, 100-149, > 150). "+
					"grouping=individual will result in a single column for each job size found.")
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterJobIDs, "jobs", "j", "",
				"Select job ids to view (comma separated list), default is all")
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
				"Specify partitions to view (comma separated list), default is all")
			sizesByWckeyCmd.Flags().BoolVarP(&FlagPrintJobCount, "printjobcount", "", false,
				" The report will print number of jobs range instead of time used")
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterNodeNames, "nodes", "n", "",
				"Specify nodes name to view (comma separated list), default is all")
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
			sizesByWckeyCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
		jobCmd.AddCommand(sizesByAccountAndWcKey)
		{
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
				"Set the job output time unit(seconds/minutes/hours, default: minutes")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterGids, "gid", "", "",
				"Select group id to view (comma separated list)")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterGrouping, "grouping", "", "50,250,500,1000",
				"Comma separated list of size groupings "+
					"(e.g. 50,100,150 would group job cpu count 1-49, 50-99, 100-149, > 150). "+
					"grouping=individual will result in a single column for each job size found.")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterJobIDs, "jobs", "j", "",
				"Select job ids to view (comma separated list), default is all")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
				"Specify partitions to view (comma separated list), default is all")
			sizesByAccountAndWcKey.Flags().BoolVarP(&FlagPrintJobCount, "printjobcount", "", false,
				" The report will print number of jobs range instead of time used")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterNodeNames, "nodes", "n", "",
				"Specify nodes name to view (comma separated list), default is all")
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
			sizesByAccountAndWcKey.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		}
	}

}
