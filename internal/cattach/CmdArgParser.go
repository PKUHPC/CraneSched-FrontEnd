package cattach

import (
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagPty            bool
	FlagInput          string
	FlagInputALL       bool
	RootCmd            = &cobra.Command{
		Use:     "cattach [flags] jobid.stepid",
		Short:   "Attach to a crane job step",
		Version: util.Version(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return MainCattach(args)
		},
	}
)

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
}
