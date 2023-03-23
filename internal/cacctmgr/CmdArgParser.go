package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var (
	FlagName       string
	FlagPartitions []string
	FlagLevel      string
	FlagNoHeader   bool
	FlagFormat     string

	FlagSetDefaultQos  string
	FlagAllowedQosList []string

	FlagPartitionFilter string

	FlagAccount protos.AccountInfo
	FlagUser    protos.UserInfo
	FlagQos     protos.QosInfo

	FlagConfigFilePath string

	rootCmd = &cobra.Command{
		Use:   "cacctmgr",
		Short: "Manage accounts, users, and qos tables",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) { //The Persistent*Run functions will be inherited by children if they do not declare their own
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			userUid = uint32(os.Getuid())
		},
	}
	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	addCmd = &cobra.Command{
		Use:   "add",
		Short: "Add entity",
		Long:  "",
	}
	addAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Add a new account",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddAccount(&FlagAccount)
		},
	}
	addUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Add a new user",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddUser(&FlagUser, FlagPartitions, FlagLevel)
		},
	}
	addQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Add a new qos",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddQos(&FlagQos)
		},
	}
	/* --------------------------------------------------- remove --------------------------------------------------- */
	removeCmd = &cobra.Command{
		Use:     "delete",
		Aliases: []string{"remove"},
		Short:   "Delete entity",
		Long:    "",
	}
	removeAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Delete an existing account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteAccount(args[0])
		},
	}
	removeUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Delete an existing user",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteUser(args[0])
		},
	}
	removeQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Delete an existing Qos",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			DeleteQos(args[0])
		},
	}
	/* --------------------------------------------------- modify  -------------------------------------------------- */
	modifyCmd = &cobra.Command{
		Use:   "modify",
		Short: "Modify entity",
		Long:  "",
	}
	modifyAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Modify account information",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().NFlag() < 2 {
				return fmt.Errorf("you must specify at least one modification item")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("description") { //See if a flag was set by the user
				ModifyAccount("description", FlagAccount.Description, FlagName, protos.ModifyEntityRequest_Overwrite)
			}
			if cmd.Flags().Changed("parent") {
				ModifyAccount("parent_account", FlagAccount.ParentAccount, FlagName, protos.ModifyEntityRequest_Overwrite)
			}
			if cmd.Flags().Changed("set_allowed_partitions") {
				ModifyAccount("allowed_partitions", strings.Join(FlagAccount.AllowedPartitions, ","), FlagName, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_partitions") {
				ModifyAccount("allowed_partitions", strings.Join(FlagAccount.AllowedPartitions, ","), FlagName, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_partitions") {
				ModifyAccount("allowed_partitions", strings.Join(FlagAccount.AllowedPartitions, ","), FlagName, protos.ModifyEntityRequest_Delete)
			}
			if cmd.Flags().Changed("set_allowed_qos_list") {
				ModifyAccount("allowed_qos_list", strings.Join(FlagAccount.AllowedQosList, ","), FlagName, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_qos_list") {
				ModifyAccount("allowed_qos_list", strings.Join(FlagAccount.AllowedQosList, ","), FlagName, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_qos_list") {
				ModifyAccount("allowed_qos_list", strings.Join(FlagAccount.AllowedQosList, ","), FlagName, protos.ModifyEntityRequest_Delete)
			}
			if cmd.Flags().Changed("default_qos") {
				ModifyAccount("default_qos", FlagAccount.DefaultQos, FlagName, protos.ModifyEntityRequest_Overwrite)
			}
		},
	}
	modifyUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Modify user information",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().NFlag() < 2 {
				return fmt.Errorf("you must specify at least one modification item")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {

			if cmd.Flags().Changed("account") { //See if a flag was set by the user
				ModifyUser("account", FlagUser.Account, FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Overwrite)
			}
			if cmd.Flags().Changed("admin_level") {
				ModifyUser("admin_level", FlagLevel, FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Overwrite)
			}
			if cmd.Flags().Changed("set_allowed_partitions") {
				ModifyUser("allowed_partitions", strings.Join(FlagPartitions, ","), FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_partitions") {
				ModifyUser("allowed_partitions", strings.Join(FlagPartitions, ","), FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_partitions") {
				ModifyUser("allowed_partitions", strings.Join(FlagPartitions, ","), FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Delete)
			}
			if cmd.Flags().Changed("set_allowed_qos_list") {
				ModifyUser("allowed_qos_list", strings.Join(FlagAllowedQosList, ","), FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_qos_list") {
				ModifyUser("allowed_qos_list", strings.Join(FlagAllowedQosList, ","), FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_qos_list") {
				ModifyUser("allowed_qos_list", strings.Join(FlagAllowedQosList, ","), FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Delete)
			}
			if cmd.Flags().Changed("default_qos") {
				ModifyUser("default_qos", FlagSetDefaultQos, FlagName, FlagPartitionFilter, protos.ModifyEntityRequest_Overwrite)
			}
		},
	}
	modifyQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Modify qos information",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().NFlag() < 2 {
				return fmt.Errorf("you must specify at least one modification item")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("description") {
				ModifyQos("description", FlagQos.Description, FlagName)
			}
			if cmd.Flags().Changed("priority") {
				ModifyQos("priority", fmt.Sprint(FlagQos.Priority), FlagName)
			}
			if cmd.Flags().Changed("max_jobs_per_user") {
				ModifyQos("max_jobs_per_user", fmt.Sprint(FlagQos.MaxJobsPerUser), FlagName)
			}
			if cmd.Flags().Changed("max_cpus_per_user") {
				ModifyQos("max_cpus_per_user", fmt.Sprint(FlagQos.MaxCpusPerUser), FlagName)
			}
			if cmd.Flags().Changed("max_time_limit_per_task") {
				ModifyQos("max_time_limit_per_task", fmt.Sprint(FlagQos.MaxTimeLimitPerTask), FlagName)
			}
		},
	}
	/* ---------------------------------------------------- show ---------------------------------------------------- */
	showCmd = &cobra.Command{
		Use:     "show",
		Aliases: []string{"list"},
		Short:   "Display all records of an entity",
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
			ShowUser("")
		},
	}
	showQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Display qos table",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			ShowQos("")
		},
	}
	/* ---------------------------------------------------- find ---------------------------------------------------- */
	findCmd = &cobra.Command{
		Use:     "find",
		Aliases: []string{"search", "query"},
		Short:   "Find a specific entity",
		Long:    "",
	}
	findAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Find and display information of a specific account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			FindAccount(args[0])
		},
	}
	findUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Find and display information of a specific user",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ShowUser(args[0])
		},
	}
	findQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Find and display information of a specific qos",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ShowQos(args[0])
		},
	}
	/* --------------------------------------------------- block ---------------------------------------------------- */
	blockCmd = &cobra.Command{
		Use:   "block",
		Short: "Block the entity so that it cannot be used",
		Long:  "",
	}
	blockAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Block an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			BlockEntity(args[0], protos.EntityType_Account, "")
		},
	}
	blockUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Block a user under an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			BlockEntity(args[0], protos.EntityType_User, FlagName)
		},
	}
	/* -------------------------------------------------- unblock --------------------------------------------------- */
	unblockCmd = &cobra.Command{
		Use:   "unblock",
		Short: "Unblock the entity",
		Long:  "",
	}
	unblockAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Unblock an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			UnblockEntity(args[0], protos.EntityType_Account, "")
		},
	}
	unblockUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Unblock a user under an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			UnblockEntity(args[0], protos.EntityType_User, FlagName)
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
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	addCmd.AddCommand(addAccountCmd)

	addAccountCmd.Flags().StringVarP(&FlagAccount.Name, "name", "N", "", "The name to identify account")
	addAccountCmd.Flags().StringVarP(&FlagAccount.Description, "description", "D", "", "Some information to describe account")
	addAccountCmd.Flags().StringVarP(&FlagAccount.ParentAccount, "parent", "P", "", "Parent account")
	addAccountCmd.Flags().StringSliceVarP(&FlagAccount.AllowedPartitions, "partition", "p", nil, "The partition list which this account has access to")
	addAccountCmd.Flags().StringVarP(&FlagAccount.DefaultQos, "default_qos", "Q", "", "Default qos of the account")
	addAccountCmd.Flags().StringSliceVarP(&FlagAccount.AllowedQosList, "qos_list", "q", nil, "Allowed qos list of the account")
	err := addAccountCmd.MarkFlagRequired("name")
	if err != nil {
		return
	}

	addCmd.AddCommand(addUserCmd)
	addUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "The name to identify user")
	addUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Parent account")
	addUserCmd.Flags().StringSliceVarP(&FlagPartitions, "partition", "p", nil, "The partition list which this account has access to")
	addUserCmd.Flags().StringVarP(&FlagLevel, "level", "L", "none", "User power level(none/operator/admin)")
	err = addUserCmd.MarkFlagRequired("name")
	if err != nil {
		return
	}
	err = addUserCmd.MarkFlagRequired("account")
	if err != nil {
		return
	}

	addCmd.AddCommand(addQosCmd)
	addQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "The name to identify qos")
	addQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Some information to describe qos")
	addQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 1000, "")
	addQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", 0, "")
	addQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", 10, "")
	addQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", 3600, "time in seconds")
	err = addQosCmd.MarkFlagRequired("name")
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
	modifyAccountCmd.Flags().StringVarP(&FlagAccount.Description, "description", "D", "", "Modify information to describe account")
	modifyAccountCmd.Flags().StringVarP(&FlagAccount.ParentAccount, "parent", "P", "", "Modify parent account")
	modifyAccountCmd.Flags().StringVarP(&FlagAccount.DefaultQos, "default_qos", "Q", "", "Modify default qos of the account")
	modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedPartitions, "set_allowed_partitions", nil, "Set the content of the allowed partition list")
	modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "set_allowed_qos_list", nil, "Set the content of the allowed qos list")
	modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedPartitions, "add_allowed_partitions", nil, "Add some new items to the allowed partition list")
	modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "add_allowed_qos_list", nil, "Add some new items to the allowed qos list")
	modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedPartitions, "delete_allowed_partitions", nil, "Delete some specific items from allowed partition list")
	modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "delete_allowed_qos_list", nil, "Delete some specific items from allowed qos list")
	modifyAccountCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Name of the account being modified")
	modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_partitions", "add_allowed_partitions", "delete_allowed_partitions")
	modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
	err = modifyAccountCmd.MarkFlagRequired("name")
	if err != nil {
		Error("Can't mark 'name' flag required")
	}

	modifyCmd.AddCommand(modifyUserCmd)
	modifyUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Modify account")
	modifyUserCmd.Flags().StringVarP(&FlagSetDefaultQos, "default_qos", "Q", "", "Modify default qos")
	modifyUserCmd.Flags().StringVarP(&FlagLevel, "admin_level", "L", "", "Modify admin level(none/operator/admin)")
	modifyUserCmd.Flags().StringSliceVar(&FlagPartitions, "set_allowed_partitions", nil, "Set the content of the allowed partition list")
	modifyUserCmd.Flags().StringSliceVar(&FlagAllowedQosList, "set_allowed_qos_list", nil, "Set the content of the allowed qos list")
	modifyUserCmd.Flags().StringSliceVar(&FlagPartitions, "add_allowed_partitions", nil, "Add some new items to the allowed partition list")
	modifyUserCmd.Flags().StringSliceVar(&FlagAllowedQosList, "add_allowed_qos_list", nil, "Add some new items to the allowed qos list")
	modifyUserCmd.Flags().StringSliceVar(&FlagPartitions, "delete_allowed_partitions", nil, "Delete some specific items from allowed partition list")
	modifyUserCmd.Flags().StringSliceVar(&FlagAllowedQosList, "delete_allowed_qos_list", nil, "Delete some specific items from allowed qos list")
	modifyUserCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Name of the user being modified")
	modifyUserCmd.Flags().StringVarP(&FlagPartitionFilter, "partition", "p", "", "Partition which being modified, if this parameter is not set explicitly, all partitions are modified by default")
	modifyUserCmd.MarkFlagsMutuallyExclusive("set_allowed_partitions", "add_allowed_partitions", "delete_allowed_partitions")
	modifyUserCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
	err = modifyUserCmd.MarkFlagRequired("name")
	if err != nil {
		Error("Can't mark 'name' flag required")
	}

	modifyCmd.AddCommand(modifyQosCmd)
	modifyQosCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Name of the qos being modified")
	err = modifyQosCmd.MarkFlagRequired("name")
	if err != nil {
		return
	}
	modifyQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Modify information to describe qos")
	modifyQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 1000, "")
	modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", 0, "")
	modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", 10, "")
	modifyQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", 3600, "time in seconds")
	/* ---------------------------------------------------- show ---------------------------------------------------- */
	rootCmd.AddCommand(showCmd)
	showCmd.AddCommand(showAccountCmd)
	showCmd.AddCommand(showUserCmd)
	showCmd.AddCommand(showQosCmd)
	showAccountCmd.Flags().BoolVarP(&FlagNoHeader, "no-header", "n", false, "no headers on output")
	showAccountCmd.Flags().StringVarP(&FlagFormat, "format", "o", "", "format specification")
	/* ---------------------------------------------------- find ---------------------------------------------------- */
	rootCmd.AddCommand(findCmd)
	findCmd.AddCommand(findAccountCmd)
	findCmd.AddCommand(findUserCmd)
	findCmd.AddCommand(findQosCmd)
	/* --------------------------------------------------- block ---------------------------------------------------- */
	rootCmd.AddCommand(blockCmd)
	blockCmd.AddCommand(blockAccountCmd)
	blockCmd.AddCommand(blockUserCmd)
	blockUserCmd.Flags().StringVarP(&FlagName, "account", "A", "", "The account where the user resides")
	/* -------------------------------------------------- unblock --------------------------------------------------- */
	rootCmd.AddCommand(unblockCmd)
	unblockCmd.AddCommand(unblockAccountCmd)
	unblockCmd.AddCommand(unblockUserCmd)
	unblockUserCmd.Flags().StringVarP(&FlagName, "account", "A", "", "The account where the user resides")
}
