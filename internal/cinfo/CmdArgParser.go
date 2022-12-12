package cinfo

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	dead       bool
	responding bool
	summarize  bool
	partitions string
	nodes      string
	states     string
	format     string
	iterate    uint64
	rootCmd    = &cobra.Command{
		Use:   "cinfo",
		Short: "display the status of all partitions and nodes",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if iterate != 0 {
				IterateQuery(iterate)
			} else {
				cinfoFun()
			}
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
func init() {
	rootCmd.Flags().BoolVarP(&dead, "dead", "d", false,
		"show only non-responding nodes")
	rootCmd.Flags().StringVarP(&partitions, "partition", "p",
		"", "report on specific partition")
	rootCmd.Flags().StringVarP(&nodes, "nodes", "n", "",
		"report on specific node(s)")
	rootCmd.Flags().StringVarP(&states, "states", "t", "",
		"specify the what states of nodes to view")
	rootCmd.Flags().BoolVarP(&responding, "responding", "r", false,
		"report only responding nodes")
	rootCmd.Flags().Uint64VarP(&iterate, "iterate", "i", 0,
		"specify an interation period in seconds")
	rootCmd.Flags().BoolVarP(&summarize, "summarize", "s", false,
		"report state summary only")
	rootCmd.Flags().StringVarP(&format, "format", "o", "",
		"format specification")
}
