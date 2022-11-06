package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"github.com/spf13/cobra"
	"os"
)

var (
	name        string
	description string
	account     string
	partition   []string
	defaultQos  string
	qosList     []string
	level       string

	modifyItem      string
	partitionFilter string

	rootCmd = &cobra.Command{
		Use:   "cacctmgr",
		Short: "Manage account in crane",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) { //The Persistent*Run functions will be inherited by children if they do not declare their own
			Init()
		},
	}
	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	addCmd = &cobra.Command{
		Use:   "add",
		Short: "Perform the add operation",
		Long:  "",
	}
	addAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Add a new account to crane",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddAccount(name, description, account, partition, defaultQos, qosList)
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
	/* --------------------------------------------------- remove --------------------------------------------------- */
	removeCmd = &cobra.Command{
		Use:     "remove",
		Aliases: []string{"delete"},
		Short:   "Perform the remove operation",
		Long:    "",
	}
	removeAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Delete existing account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteAccount(args[0])
		},
	}
	removeUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Delete existing user",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteUser(args[0])
		},
	}
	removeQosCmd = &cobra.Command{
		Use:   "Qos",
		Short: "Delete existing Qos",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteQos(args[0])
		},
	}
	/* --------------------------------------------------- modify  -------------------------------------------------- */
	modifyCmd = &cobra.Command{
		Use:   "modify",
		Short: "Perform the modify operation",
		Long:  "",
	}
	modifyAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Modify account information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("set") { //See if a flag was set by the user
				ModifyAccount(modifyItem, name, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add") {
				ModifyAccount(modifyItem, name, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete") {
				ModifyAccount(modifyItem, name, protos.ModifyEntityRequest_Delete)
			}
		},
	}
	modifyUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Modify user information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			//itemLeft, itemRight := ParseEquation(modifyItem)
			//filterLeft, filterRight := ParseEquation(modifyFilter)
			if cmd.Flags().Changed("set") { //See if a flag was set by the user
				ModifyUser(modifyItem, name, partitionFilter, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add") {
				ModifyUser(modifyItem, name, partitionFilter, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete") {
				ModifyUser(modifyItem, name, partitionFilter, protos.ModifyEntityRequest_Delete)
			}
		},
	}
	/* ---------------------------------------------------- show ---------------------------------------------------- */
	showCmd = &cobra.Command{
		Use:     "show",
		Aliases: []string{"list"},
		Short:   "Perform the show operation",
		Long:    "",
	}
	showAccountCmd = &cobra.Command{
		Use:     "account",
		Aliases: []string{"accounts"},
		Short:   "Display account tree and account details",
		Long:    "",
		Run: func(cmd *cobra.Command, args []string) {
			ShowAccounts()
		},
	}
	showUserCmd = &cobra.Command{
		Use:     "user",
		Aliases: []string{"users"},
		Short:   "Display user table",
		Long:    "",
		Run: func(cmd *cobra.Command, args []string) {
			ShowUsers()
		},
	}
	/* ---------------------------------------------------- find ---------------------------------------------------- */
	findCmd = &cobra.Command{
		Use:     "find",
		Aliases: []string{"search"},
		Short:   "Perform the search operation",
		Long:    "",
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
	addAccountCmd.Flags().StringVarP(&description, "description", "D", "", "some information to describe account")
	addAccountCmd.Flags().StringVarP(&account, "parent", "P", "", "parent account")
	addAccountCmd.Flags().StringSliceVar(&partition, "partition", nil, "the partition list which this account has access to")
	addAccountCmd.Flags().StringVarP(&defaultQos, "default_qos", "Q", "", "Default qos of the account")
	addAccountCmd.Flags().StringSliceVar(&qosList, "qos_list", nil, "Allowed qos list of the account")
	err := addAccountCmd.MarkFlagRequired("name")
	if err != nil {
		return
	}

	addCmd.AddCommand(addUserCmd)
	addUserCmd.Flags().StringVarP(&name, "name", "N", "", "the name to identify user")
	addUserCmd.Flags().StringVarP(&account, "account", "A", "", "parent account")
	addUserCmd.Flags().StringSliceVar(&partition, "partition", nil, "the partition list which this account has access to")
	addUserCmd.Flags().StringVarP(&level, "level", "L", "none", "user power level")
	err = addUserCmd.MarkFlagRequired("name")
	if err != nil {
		return
	}
	err = addUserCmd.MarkFlagRequired("account")
	if err != nil {
		return
	}

	/* --------------------------------------------------- remove --------------------------------------------------- */
	rootCmd.AddCommand(removeCmd)
	removeCmd.AddCommand(removeAccountCmd)
	removeCmd.AddCommand(removeUserCmd)
	removeCmd.AddCommand(removeQosCmd)
	/* --------------------------------------------------- modify  -------------------------------------------------- */
	rootCmd.AddCommand(modifyCmd)

	modifyCmd.AddCommand(modifyAccountCmd)
	modifyAccountCmd.Flags().StringVarP(&modifyItem, "set", "S", "", "Modify as an overlay")
	modifyAccountCmd.Flags().StringVarP(&modifyItem, "add", "A", "", "Modify as an addition")
	modifyAccountCmd.Flags().StringVarP(&modifyItem, "delete", "D", "", "Modify as an deletion")
	modifyAccountCmd.Flags().StringVarP(&name, "name", "N", "", "Name of the account being modified")
	modifyAccountCmd.MarkFlagsMutuallyExclusive("set", "add", "delete")
	err = modifyAccountCmd.MarkFlagRequired("name")
	if err != nil {
		Error("Can't mark 'name' flag required")
	}

	modifyCmd.AddCommand(modifyUserCmd)
	modifyUserCmd.Flags().StringVarP(&modifyItem, "set", "S", "", "Modify as an overlay")
	modifyUserCmd.Flags().StringVarP(&modifyItem, "add", "A", "", "Modify as an addition")
	modifyUserCmd.Flags().StringVarP(&modifyItem, "delete", "D", "", "Modify as an deletion")
	modifyUserCmd.Flags().StringVarP(&name, "name", "N", "", "Name of the user being modified")
	modifyUserCmd.Flags().StringVarP(&partitionFilter, "partition", "P", "", "partition which being modified")
	modifyUserCmd.MarkFlagsMutuallyExclusive("set", "add", "delete")
	err = modifyUserCmd.MarkFlagRequired("name")
	if err != nil {
		Error("Can't mark 'name' flag required")
	}
	/* ---------------------------------------------------- show ---------------------------------------------------- */
	rootCmd.AddCommand(showCmd)
	showCmd.AddCommand(showAccountCmd)
	showCmd.AddCommand(showUserCmd)
	/* ---------------------------------------------------- find ---------------------------------------------------- */
	rootCmd.AddCommand(findCmd)
	findCmd.AddCommand(findAccountCmd)
	findCmd.AddCommand(findUserCmd)
}
