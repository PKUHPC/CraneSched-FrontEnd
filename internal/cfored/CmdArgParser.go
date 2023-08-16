package cfored

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
)

var (
	FlagConfigFilePath string
	FlagDebugLevel     string
)

func ParseCmdArgs() {
	rootCmd := &cobra.Command{
		Use:   "cfored",
		Short: "Daemon for interactive job management",
		Run: func(cmd *cobra.Command, args []string) {
			StartCfored()
		},
	}

	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	rootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "D",
		"info", "Output level")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
