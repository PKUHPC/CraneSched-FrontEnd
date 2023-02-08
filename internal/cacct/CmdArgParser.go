package cacct

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
)

var (
	FlagConfigFilePath string

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
}
