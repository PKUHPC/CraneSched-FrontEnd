package cacct

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
)

var (
	FlagConfigFilePath string
	FlagFormat         string
	FlagSetStartTime   string
	FlagSetEndTime     string
	FlagFilterAccounts string
	FlagFilterJobIDs   string
	FlagFilterUsers    string
	FlagFilterJobNames string
	FlagNoHeader       bool

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
	rootCmd.Flags().StringVarP(&FlagSetEndTime, "endtime", "E",
		"", "Select jobs eligible before this time. ")
	rootCmd.Flags().StringVarP(&FlagSetStartTime, "startime", "S",
		"", " Select jobs eligible after this time ")
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
	rootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "", "format specification")
}
