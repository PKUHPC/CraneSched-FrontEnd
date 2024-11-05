package clogin

import (
	"CraneFrontEnd/internal/util"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

var (
	FlagUserName       string
	FlagPassword       string
	FlagConfigFilePath string

	RootCmd = &cobra.Command{
		Use:     "login [flags]",
		Short:   "Login with your username and password",
		Long:    "",
		Version: util.Version(),
		Args: func(cmd *cobra.Command, args []string) error {
			if FlagUserName == "" {
				log.Error("Username must be provided")
				os.Exit(util.ErrorCmdArg)
			}

			if FlagPassword == "" {
				log.Error("Password must be provided")
				os.Exit(util.ErrorCmdArg)
			}

			return nil
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := Login(FlagUserName, FlagPassword); err != util.ErrorSuccess {
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
	RootCmd.Flags().StringVarP(&FlagUserName, "name", "N", "", "Set the name of the user")
	RootCmd.Flags().StringVarP(&FlagPassword, "password", "P", "", "Set the password of the user")
}
