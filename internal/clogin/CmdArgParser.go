package clogin

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"

	"golang.org/x/crypto/ssh/terminal"

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
		Args: func(cmd *cobra.Command, args []string) error {
			fmt.Print("Enter Password: ")
			bytePassword, err := terminal.ReadPassword(int(os.Stdin.Fd()))
			if err != nil {
				fmt.Println("\nError reading password:", err)
				os.Exit(1)
			}
			FlagPassword = string(bytePassword)
			fmt.Println()

			return nil
		},
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
}
