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
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath  string
	FlagJson            bool
	FlagFilterStartTime string
	FlagFilterEndTime   string
	FlagFilterAccounts  string
	FlagFilterUsers     string
	FlagFilterQoss      string
	FlagOutType         string
	FlagTopCount        uint32
	FlagGroups          string
	FlagFilterWckeys    string

	RootCmd = &cobra.Command{
		Use:     "creport",
		Short:   "Display system job report",
		Long:    "",
		Version: util.Version(),
		Args:    cobra.MaximumNArgs(1),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			return err
		},
	}

	userCmd = &cobra.Command{
		Use:   "user",
		Short: "Display system user job report",
		Long:  "",
	}

	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Display system cluster job report",
		Long:  "",
	}

	jobCmd = &cobra.Command{
		Use:   "job",
		Short: "Display system  job size report",
		Long:  "",
	}

	userTopUsageCmd = &cobra.Command{
		Use:   "topusage",
		Short: "",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryUsersTopSummaryItem()
		},
	}

	accountUtilizationByUserCmd = &cobra.Command{
		Use:   "accountutilizationbyusercmd",
		Short: "Display statistical information for all job users under the specified account, as of the end of the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserSummaryItem(CheckAccountUserStatus)
		},
	}
	userUtilizationByAccountCmd = &cobra.Command{
		Use:   "userutilizationbyaccount",
		Short: "Display statistical information for all job account under the specified user, as of the end of the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserSummaryItem(CheckUserAccountStatus)
		},
	}
	userUtilizationByWckeyCmd = &cobra.Command{
		Use:   "userutilizationbywckey",
		Short: "Display the statistical information of all job wckeys under the specified user that ended at the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserWckeySummaryItem(CheckUserWckeyStatus)
		},
	}
	wckeyUtilizationByUserCmd = &cobra.Command{
		Use:   "wckeyutilizationbyuser",
		Short: "Display the statistical information of all job users under the specified user that ended at the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserWckeySummaryItem(CheckWckeyUserStatus)
		},
	}
	accountUtilizationByQosCmd = &cobra.Command{
		Use:   "accountutilizationbyqos",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserSummaryItem(CheckAccountQosStatus)
		},
	}
	utilizationCmd = &cobra.Command{
		Use:   "utilization",
		Short: "Display relevant cluster parameters such as cluster nodes",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserSummaryItem(CheckClusterStatus)
		},
	}
	sizesByAccountCmd = &cobra.Command{
		Use:   "sizesbyaccount",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserWckeySummaryItem(CheckAccountCpusStatus)
		},
	}
	sizesByWckeyCmd = &cobra.Command{
		Use:   "sizesbywckey",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserWckeySummaryItem(CheckWckeyCpusStatus)
		},
	}
	sizesByAccountAndWcKey = &cobra.Command{
		Use:   "sizesbyaccountandwckey",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryAccountUserWckeySummaryItem(CheckAccountWckeyCpusStatus)
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
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userTopUsageCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userTopUsageCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			userTopUsageCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userTopUsageCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
			userTopUsageCmd.Flags().Uint32VarP(&FlagTopCount, "topcount", "", 10, "Change the number of users displayed, default is 10 ")
			userTopUsageCmd.Flags().StringVarP(&FlagGroups, "group", "", "",
				"Group all accounts together for each user")
		}

	}
	RootCmd.AddCommand(clusterCmd)
	{
		clusterCmd.AddCommand(accountUtilizationByUserCmd)
		{
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}
		clusterCmd.AddCommand(userUtilizationByAccountCmd)
		{
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}
		clusterCmd.AddCommand(userUtilizationByWckeyCmd)
		{
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
		}
		clusterCmd.AddCommand(wckeyUtilizationByUserCmd)
		{
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}
		clusterCmd.AddCommand(accountUtilizationByQosCmd)
		{
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterQoss, "qos", "",
				"", "Filter jobs with Qos",
			)
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}
		clusterCmd.AddCommand(utilizationCmd)
		{
			utilizationCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			utilizationCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			utilizationCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}

	}
	RootCmd.AddCommand(jobCmd)
	{
		jobCmd.AddCommand(sizesByAccountCmd)
		{
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesByAccountCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesByAccountCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}
		jobCmd.AddCommand(sizesByWckeyCmd)
		{
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesByWckeyCmd.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}
		jobCmd.AddCommand(sizesByAccountAndWcKey)
		{
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagOutType, "time", "t", "seconds",
				"Select users to view (comma separated list)")
		}
	}

}
