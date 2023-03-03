package cinfo

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
)

var (
	FlagFilterDownOnly       bool
	FlagFilterRespondingOnly bool
	FlagFilterPartitions     string
	FlagFilterNodes          string
	FlagNodesOnCentricFormat bool
	FlagFilterCranedStates   string
	FlagSummarize            bool
	FlagFormat               string
	FlagIterate              uint64
	FlagConfigFilePath       string

	RootCmd = &cobra.Command{
		Use:   "cinfo",
		Short: "display the status of all partitions and nodes",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if FlagIterate != 0 {
				loopedQuery(FlagIterate)
			} else {
				cinfoFunc()
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
	RootCmd.Flags().BoolVarP(&FlagFilterDownOnly, "dead", "d", false,
		"show only non-responding nodes")
	RootCmd.Flags().StringVarP(&FlagFilterPartitions, "partition", "p",
		"", "report on specific partition")
	RootCmd.Flags().StringVarP(&FlagFilterNodes, "nodes", "n", "",
		"report on specific node(s)")
	RootCmd.Flags().StringVarP(&FlagFilterCranedStates, "states", "t", "",
		"Include craned nodes only with certain states. \n"+
			"The state can take IDLE, MIX, ALLOC and DOWN and is case-insensitive. \n"+
			"Example: \n"+
			"\t -t IDLE,mIx \n"+
			"\t -t=Alloc \n")
	RootCmd.Flags().BoolVarP(&FlagFilterRespondingOnly, "responding", "r", false,
		"report only responding nodes")
	RootCmd.Flags().Uint64VarP(&FlagIterate, "iterate", "i", 0,
		"specify an interval in seconds")
	RootCmd.Flags().BoolVarP(&FlagSummarize, "summarize", "s", false,
		"report state summary only")
	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		"format specification")
	RootCmd.Flags().BoolVarP(&FlagNodesOnCentricFormat, "Node", "N", false,
		"report on node centric format")
}
