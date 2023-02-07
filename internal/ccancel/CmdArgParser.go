package ccancel

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"regexp"
)

var (
	FlagTaskName       string //单个.
	FlagPartition      string //单个.
	FlagState          string //单个. 默认值
	FlagAccount        string //单个.
	FlagUserName       string //单个.
	FlagNodes          string //多个
	FlagConfigFilePath string

	rootCmd = &cobra.Command{
		Use:   "ccancel [<task id>[[,<task id>]...]] [options]",
		Short: "cancel pending or running tasks",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.MaximumNArgs(1)(cmd, args)
			if err != nil {
				return err
			}

			if len(args) == 0 &&
				FlagTaskName == "" &&
				FlagPartition == "" &&
				FlagState == "" &&
				FlagAccount == "" &&
				FlagUserName == "" &&
				FlagNodes == "" {
				return fmt.Errorf("at least one condition should be given")
			}

			if len(args) > 0 {
				matched, _ := regexp.MatchString(`^([1-9][0-9]*)(,[1-9][0-9]*)*$`, args[0])
				if !matched {
					return fmt.Errorf("task id list must follow the format " +
						"<task id> or '<task id>,<task id>,<task id>...'")
				}
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// args was checked by cobra.ExactArgs(1)
			// len(args)=1 here.
			CancelTask(args)
		},
	}
)

func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", "/etc/crane/config.yaml", "Path to configuration file")
	rootCmd.Flags().StringVarP(&FlagTaskName, "name", "n", "",
		"cancel tasks only with the task name")
	rootCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "",
		"cancel tasks jobs only in the Partition")
	rootCmd.Flags().StringVarP(&FlagState, "state", "t", "",
		"cancel tasks of the State. "+
			"Valid task states are PENDING(PD), RUNNING(R). "+
			"Task states are case-insensitive.")
	rootCmd.Flags().StringVarP(&FlagAccount, "account", "A", "",
		"cancel tasks under the FlaAccount")
	rootCmd.Flags().StringVarP(&FlagUserName, "user", "u", "",
		"cancel tasks run by the user")
	rootCmd.Flags().StringVarP(&FlagNodes, "nodes", "w", "",
		"cancel tasks running on the nodes")

	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
}
