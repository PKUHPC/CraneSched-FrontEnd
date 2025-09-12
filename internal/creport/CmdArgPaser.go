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
	FlagOutType         string

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
			dbConfig, err = GetInfluxDbConfig(config)
			return err
		},
	}
	initCmd = &cobra.Command{
		Use:   "init",
		Short: "init report",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			Init()
		},
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

	accountUtilizationByUserCmd = &cobra.Command{
		Use:   "AccountUtilizationByUserCmd",
		Short: "Display statistical information for all job users under the specified account, as of the end of the specified time",
		Long:  "",
		//Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// if err := QueryAccountUserSummaryItem(); err != nil {
			// 	os.Exit(util.ErrorCmdArg)
			// }
			QueryAccountUserSummaryItem()
			//QueryAccountUserSummaryItemInfluxdb()

		},
	}
	userutilizationbyaccount = &cobra.Command{
		Use:   "Userutilizationbyaccount",
		Short: "Display statistical information for all job account under the specified user, as of the end of the specified time",
		Long:  "",
	}
	userutilizationbywckey = &cobra.Command{
		Use:   "Userutilizationbywckey",
		Short: "Display the statistical information of all job wckeys under the specified user that ended at the specified time",
		Long:  "",
	}
	wckeyutilizationbyuser = &cobra.Command{
		Use:   "Wckeyutilizationbyuser",
		Short: "Display the statistical information of all job users under the specified user that ended at the specified time",
		Long:  "",
	}
	accountUtilizationByQOS = &cobra.Command{
		Use:   "AccountUtilizationByQOS",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
	}
	utilization = &cobra.Command{
		Use:   "utilization",
		Short: "Display relevant cluster parameters such as cluster nodes",
		Long:  "",
	}
	sizesbyaccount = &cobra.Command{
		Use:   "sizesbyaccount",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
	}
	sizesbywckey = &cobra.Command{
		Use:   "sizesbywckey ",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
	}
	SizesByAccountAndWcKey = &cobra.Command{
		Use:   "SizesByAccountAndWcKey ",
		Short: "Display the statistical information of all job qos under the specified account that ended at the specified time",
		Long:  "",
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
	RootCmd.AddCommand(initCmd)
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
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagOutType, "time", "t", "Minutes",
				"Select users to view (comma separated list)")
		}
		clusterCmd.AddCommand(userutilizationbyaccount)
		{
			userutilizationbyaccount.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userutilizationbyaccount.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}
		clusterCmd.AddCommand(userutilizationbywckey)
		{
			userutilizationbywckey.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			userutilizationbywckey.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}
		clusterCmd.AddCommand(wckeyutilizationbyuser)
		{
			wckeyutilizationbyuser.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			wckeyutilizationbyuser.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}
		clusterCmd.AddCommand(accountUtilizationByQOS)
		{
			accountUtilizationByQOS.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			accountUtilizationByQOS.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}
		clusterCmd.AddCommand(utilization)
		{
			utilization.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			utilization.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}

	}
	RootCmd.AddCommand(jobCmd)
	{
		jobCmd.AddCommand(sizesbyaccount)
		{
			sizesbyaccount.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesbyaccount.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}
		jobCmd.AddCommand(sizesbywckey)
		{
			sizesbywckey.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			sizesbywckey.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}
		jobCmd.AddCommand(SizesByAccountAndWcKey)
		{
			SizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
				"", "Filter jobs with an end time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
			SizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
				"", "Filter jobs with a start time within a certain time period, which can use closed intervals"+
					"(timeFormat: 2024-01-02T15:04:05~2024-01-11T11:12:41)",
			)
		}
	}

}
