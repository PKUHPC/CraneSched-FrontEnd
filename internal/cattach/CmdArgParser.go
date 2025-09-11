package cattach

import (
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagPty            bool
	FlagErrorFilter    uint32
	FlagInputFilter    uint32
	FlagOutputFilter   uint32
	FlagLabel          bool
	FlagLayout         bool
	FlagQuiet          bool
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
	RootCmd.PersistentFlags().Uint32Var(&FlagErrorFilter, "error-filter", 0, "only print stderr from the specified task")
	RootCmd.PersistentFlags().Uint32Var(&FlagOutputFilter, "output-filter", 0, "only print stdout from the specified task")
	RootCmd.PersistentFlags().Uint32Var(&FlagInputFilter, "input-filter", 0, "send stdin to only the specified task")
	RootCmd.PersistentFlags().BoolVar(&FlagLabel, "label", false, "prepend task number to lines of stdout & stderr")
	RootCmd.PersistentFlags().BoolVar(&FlagLayout, "layout", false, "print task layout info and exit (does not attach to tasks)")
	RootCmd.PersistentFlags().BoolVar(&FlagQuiet, "quiet", false, "quiet mode (suppress informational messages)")
}
