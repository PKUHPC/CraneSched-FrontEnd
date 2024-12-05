package clogin

import (
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
	FlagPassword       string
	FlagConfigFilePath string

	RootCmd = &cobra.Command{
		Use:     "login [flags]",
		Short:   "Login with your password",
		Long:    "",
		Version: util.Version(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			userUid = uint32(os.Getuid())
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := Login(FlagPassword); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().StringVarP(&FlagPassword, "password", "P", "", "Input the password of the user")
}
