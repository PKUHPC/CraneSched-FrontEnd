package cfored

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string

	RootCmd = &cobra.Command{
		Use:   "calloc",
		Short: "Allocate resource and start a new shell",
		Run: func(cmd *cobra.Command, args []string) {
			StartCfored()
		},
	}
)

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
}
