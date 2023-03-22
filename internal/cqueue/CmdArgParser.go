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
	FlagFilterQos        string //to use
	FlagFilterStates     string
	FlagFilterUsers      string
	FlagFilterAccounts   string
	FlagIterate          uint64

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
}
