package cacctmgr

import (
	"github.com/spf13/cobra"
	"os"
)

var (
	name      string
	describe  string
	account   string
	partition []string
	Qos       string
	level     string

	rootCmd = &cobra.Command{
		Use:   "cacctmgr",
		Short: "A command to manage account in crane",
		Long:  "",
	}
	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	addCmd = &cobra.Command{
		Use:   "add",
		Short: "A command to perform the add operation",
		Long:  "",
	}
	addAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Add a new account to crane",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddAccount(name, describe, account, partition, Qos)
		},
	}
	addUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Add a new user to crane",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddUser(name, account, partition, level)
		},
	}
	/* --------------------------------------------------- delete --------------------------------------------------- */
	deleteCmd = &cobra.Command{
		Use:   "delete",
		Short: "A command to perform the remove operation",
		Long:  "",
	}
	deleteAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Delete existing account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteAccount(args[0])
		},
	}
	deleteUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Delete existing user",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteUser(args[0])
		},
	}
	deleteQosCmd = &cobra.Command{
		Use:   "Qos",
		Short: "Delete existing Qos",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteQos(args[0])
		},
	}
	/* ---------------------------------------------------- set  ---------------------------------------------------- */
	setCmd = &cobra.Command{
		Use:   "set",
		Short: "A command to perform the modify operation",
		Long:  "",
	}
	setAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Modify account information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("parent") { //See if a flag was set by the user
				SetAccount(name, describe, &account, Qos)
			} else {
				SetAccount(name, describe, nil, Qos)
			}
		},
	}
	setUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Modify user information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("level") { //See if a flag was set by the user
				SetUser(name, account, &level)
			} else {
				SetUser(name, account, nil)
			}
		},
	}
	/* ---------------------------------------------------- show ---------------------------------------------------- */
	showCmd = &cobra.Command{
		Use:   "show",
		Short: "A command to perform the show operation",
		Long:  "",
	}
	showAccountCmd = &cobra.Command{
		Use:   "accounts",
		Short: "Display account tree and account details",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			ShowAccounts()
		},
	}
	showUserCmd = &cobra.Command{
		Use:   "users",
		Short: "Display user table",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			ShowUsers()
		},
	}
	/* ---------------------------------------------------- find ---------------------------------------------------- */
	findCmd = &cobra.Command{
		Use:   "find",
		Short: "A command to perform the search operation",
		Long:  "",
	}
	findAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Find and display a specific account information",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			FindAccount(args[0])
		},
	}
	findUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Find and display a specific user information",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			FindUser(args[0])
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

	rootCmd.AddCommand(addCmd)
	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	addCmd.AddCommand(addAccountCmd)
	addAccountCmd.Flags().StringVarP(&name, "name", "N", "", "the name to identify account")
	addAccountCmd.Flags().StringVarP(&describe, "describe", "D", "", "some information to describe account")
	addAccountCmd.Flags().StringVar(&account, "parent", "", "parent account")
	addAccountCmd.Flags().StringArrayVar(&partition, "partition", nil, "the partition list which this account has access to")
	addAccountCmd.Flags().StringVarP(&Qos, "Qos", "Q", "", "QOS of the account")

	addCmd.AddCommand(addUserCmd)
	addUserCmd.Flags().StringVarP(&name, "name", "N", "", "the name to identify user")
	addUserCmd.Flags().StringVarP(&account, "account", "A", "", "parent account")
	addUserCmd.Flags().StringArrayVar(&partition, "partition", nil, "the partition list which this account has access to")
	addUserCmd.Flags().StringVarP(&level, "level", "L", "none", "user power level")
	/* --------------------------------------------------- delete --------------------------------------------------- */
	rootCmd.AddCommand(deleteCmd)
	deleteCmd.AddCommand(deleteAccountCmd)
	deleteCmd.AddCommand(deleteUserCmd)
	deleteCmd.AddCommand(deleteQosCmd)
	/* ---------------------------------------------------- set  ---------------------------------------------------- */
	rootCmd.AddCommand(setCmd)

	setCmd.AddCommand(setAccountCmd)
	setAccountCmd.Flags().StringVarP(&name, "name", "N", "", "the name to identify account")
	setAccountCmd.Flags().StringVarP(&describe, "describe", "D", "", "some information to describe account")
	setAccountCmd.Flags().StringVar(&account, "parent", "", "parent account")
	//setAccountCmd.Flags().StringArrayVar(&partition, "partition", nil, "the partition list which this account has access to")
	setAccountCmd.Flags().StringVarP(&Qos, "Qos", "Q", "", "QOS of the account")

	setCmd.AddCommand(setUserCmd)
	setUserCmd.Flags().StringVarP(&name, "name", "N", "", "the name to identify user")
	setUserCmd.Flags().StringVarP(&account, "account", "A", "", "parent account")
	//addUserCmd.Flags().StringArrayVar(&partition, "partition", nil, "the partition list which this account has access to")
	setUserCmd.Flags().StringVarP(&level, "level", "L", "none", "user power level")
	/* ---------------------------------------------------- show ---------------------------------------------------- */
	rootCmd.AddCommand(showCmd)
	showCmd.AddCommand(showAccountCmd)
	showCmd.AddCommand(showUserCmd)
	/* ---------------------------------------------------- find ---------------------------------------------------- */
	rootCmd.AddCommand(findCmd)
	findCmd.AddCommand(findAccountCmd)
	findCmd.AddCommand(findUserCmd)
}
