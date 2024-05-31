/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"math"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagName        string
	FlagPartitions  []string
	FlagLevel       string
	FlagQosName     string
	FlagAccountName string
	FlagForce       bool
	FlagNoHeader    bool
	FlagFormat      string
	FlagCoordinate  bool

	FlagSetDefaultQos  string
	FlagAllowedQosList []string

	FlagPartition string

	FlagAccount protos.AccountInfo
	FlagUser    protos.UserInfo
	FlagQos     protos.QosInfo

	FlagConfigFilePath string

	RootCmd = &cobra.Command{
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
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := AddAccount(&FlagAccount); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	addUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Add a new user",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := AddUser(&FlagUser, FlagPartitions, FlagLevel, FlagCoordinate); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	addQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Add a new qos",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := AddQos(&FlagQos); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
			if err := DeleteAccount(args[0]); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	removeUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Delete an existing user",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := DeleteUser(args[0], FlagName); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	removeQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Delete an existing Qos",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := DeleteQos(args[0]); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
			err := util.ErrorSuccess
			if cmd.Flags().Changed("description") { //See if a flag was set by the user
				err = ModifyAccount("description", FlagAccount.Description, FlagName, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			//if cmd.Flags().Changed("parent") {
			//	ModifyAccount("parent_account", FlagAccount.ParentAccount, FlagName, protos.ModifyEntityRequest_Overwrite)
			//}
			if cmd.Flags().Changed("set_allowed_partition") {
				err = ModifyAccount("allowed_partition", strings.Join(FlagAccount.AllowedPartitions, ","), FlagName, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_partition") {
				err = ModifyAccount("allowed_partition", FlagPartition, FlagName, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_partition") {
				err = ModifyAccount("allowed_partition", FlagPartition, FlagName, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set_allowed_qos_list") {
				err = ModifyAccount("allowed_qos_list", strings.Join(FlagAccount.AllowedQosList, ","), FlagName, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_qos_list") {
				err = ModifyAccount("allowed_qos_list", strings.Join(FlagAccount.AllowedQosList, ","), FlagName, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_qos_list") {
				err = ModifyAccount("allowed_qos_list", strings.Join(FlagAccount.AllowedQosList, ","), FlagName, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("default_qos") {
				err = ModifyAccount("default_qos", FlagAccount.DefaultQos, FlagName, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
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
			err := util.ErrorSuccess
			if cmd.Flags().Changed("admin_level") { //See if a flag was set by the user
				err = ModifyUser("admin_level", FlagLevel, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set_allowed_partition") {
				err = ModifyUser("allowed_partition", strings.Join(FlagPartitions, ","), FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_partition") {
				err = ModifyUser("allowed_partition", strings.Join(FlagPartitions, ","), FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_partition") {
				err = ModifyUser("allowed_partition", strings.Join(FlagPartitions, ","), FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set_allowed_qos_list") {
				err = ModifyUser("allowed_qos_list", strings.Join(FlagAllowedQosList, ","), FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_qos_list") {
				err = ModifyUser("allowed_qos_list", FlagQosName, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_qos_list") {
				err = ModifyUser("allowed_qos_list", FlagQosName, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("default_qos") {
				err = ModifyUser("default_qos", FlagSetDefaultQos, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
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
				if err := ModifyQos("description", FlagQos.Description, FlagName); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("priority") {
				if err := ModifyQos("priority", fmt.Sprint(FlagQos.Priority), FlagName); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max_jobs_per_user") {
				if err := ModifyQos("max_jobs_per_user", fmt.Sprint(FlagQos.MaxJobsPerUser), FlagName); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max_cpus_per_user") {
				if err := ModifyQos("max_cpus_per_user", fmt.Sprint(FlagQos.MaxCpusPerUser), FlagName); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max_time_limit_per_task") {
				if err := ModifyQos("max_time_limit_per_task", fmt.Sprint(FlagQos.MaxTimeLimitPerTask), FlagName); err != util.ErrorSuccess {
					os.Exit(err)
				}
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
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowAccounts(); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	showUserCmd = &cobra.Command{
		Use:     "user",
		Aliases: []string{"users"},
		Short:   "Display user table",
		Long:    "",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowUser("", FlagAccountName); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	showQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Display qos table",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowQos(""); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
			if err := FindAccount(args[0]); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	findUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Find and display information of a specific user",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowUser(args[0], FlagAccountName); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	findQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Find and display information of a specific qos",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowQos(args[0]); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
			if err := BlockAccountOrUser(args[0], protos.EntityType_Account, ""); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	blockUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Block a user under an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := BlockAccountOrUser(args[0], protos.EntityType_User, FlagName); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
			if err := UnblockAccountOrUser(args[0], protos.EntityType_Account, ""); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	unblockUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Unblock a user under an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := UnblockAccountOrUser(args[0], protos.EntityType_User, FlagName); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorExecuteFailed)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")

	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	RootCmd.AddCommand(addCmd)
	{
		addCmd.AddCommand(addAccountCmd)
		{
			addAccountCmd.Flags().StringVarP(&FlagAccount.Name, "name", "N", "", "The name to identify account")
			addAccountCmd.Flags().StringVarP(&FlagAccount.Description, "description", "D", "", "Some information to describe account")
			addAccountCmd.Flags().StringVarP(&FlagAccount.ParentAccount, "parent", "P", "", "Parent account")
			addAccountCmd.Flags().StringSliceVarP(&FlagAccount.AllowedPartitions, "partition", "p", nil, "The partition list which this account has access to")
			addAccountCmd.Flags().StringVarP(&FlagAccount.DefaultQos, "default_qos", "Q", "", "Default qos of the account")
			addAccountCmd.Flags().StringSliceVarP(&FlagAccount.AllowedQosList, "qos_list", "q", nil, "Allowed qos list of the account")
			if err := addAccountCmd.MarkFlagRequired("name"); err != nil {
				return
			}
		}

		addCmd.AddCommand(addUserCmd)
		{
			addUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "The name to identify user")
			addUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Parent account")
			addUserCmd.Flags().StringSliceVarP(&FlagPartitions, "partition", "p", nil, "The partition list which this account has access to")
			addUserCmd.Flags().StringVarP(&FlagLevel, "level", "L", "none", "User power level (none/operator)")
			addUserCmd.Flags().BoolVarP(&FlagCoordinate, "coordinate", "c", false, "Set whether the user is the coordinator of the parent account")
			if err := addUserCmd.MarkFlagRequired("name"); err != nil {
				return
			}
			if err := addUserCmd.MarkFlagRequired("account"); err != nil {
				return
			}
		}

		addCmd.AddCommand(addQosCmd)
		{
			addQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "The name to identify qos")
			addQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Some information to describe qos")
			addQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 0, "")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", math.MaxUint32, "")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", math.MaxUint32, "")
			addQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", uint64(util.InvalidDuration().Seconds), "time in seconds")
			if err := addQosCmd.MarkFlagRequired("name"); err != nil {
				return
			}
		}
	}

	/* --------------------------------------------------- remove --------------------------------------------------- */
	RootCmd.AddCommand(removeCmd)
	{
		removeCmd.AddCommand(removeAccountCmd)
		removeCmd.AddCommand(removeQosCmd)

		removeCmd.AddCommand(removeUserCmd)
		{
			removeUserCmd.Flags().StringVarP(&FlagName, "account", "A", "", "Remove user from this account")
		}

		removeCmd.SetUsageTemplate(`Usage:
  cacctmgr {{.Use}} [name]

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}
`)
	}

	/* --------------------------------------------------- modify  -------------------------------------------------- */
	RootCmd.AddCommand(modifyCmd)
	{
		modifyCmd.AddCommand(modifyAccountCmd)
		{
			modifyAccountCmd.Flags().StringVarP(&FlagAccount.Description, "description", "D", "", "Modify information to describe account")
			//modifyAccountCmd.Flags().StringVarP(&FlagAccount.ParentAccount, "parent", "P", "", "Modify parent account")
			modifyAccountCmd.Flags().StringVarP(&FlagAccount.DefaultQos, "default_qos", "Q", "", "Modify default qos of the account")
			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedPartitions, "set_allowed_partition", nil, "Set the content of the allowed partition list")
			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "set_allowed_qos_list", nil, "Set the content of the allowed qos list")
			modifyAccountCmd.Flags().StringVar(&FlagPartition, "add_allowed_partition", "", "Add a new item to the allowed partition list")
			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "add_allowed_qos_list", nil, "Add some new items to the allowed qos list")
			modifyAccountCmd.Flags().StringVar(&FlagPartition, "delete_allowed_partition", "", "Delete a specific item from allowed partition list")
			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "delete_allowed_qos_list", nil, "Delete some specific items from allowed qos list")
			modifyAccountCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Name of the account being modified")
			modifyAccountCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced operation")
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_partition", "add_allowed_partition", "delete_allowed_partition")
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
			if err := modifyAccountCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalf("Can't mark 'name' flag required")
			}
		}

		modifyCmd.AddCommand(modifyUserCmd)
		{
			modifyUserCmd.Flags().StringVarP(&FlagSetDefaultQos, "default_qos", "Q", "", "Modify default qos")
			modifyUserCmd.Flags().StringVarP(&FlagLevel, "admin_level", "L", "", "Modify admin level(none/operator/admin)")
			modifyUserCmd.Flags().StringSliceVar(&FlagPartitions, "set_allowed_partition", nil, "Set the content of the allowed partition list")
			modifyUserCmd.Flags().StringSliceVar(&FlagAllowedQosList, "set_allowed_qos_list", nil, "Set the content of the allowed qos list")
			modifyUserCmd.Flags().StringSliceVar(&FlagPartitions, "add_allowed_partition", nil, "Add some new items to the allowed partition list")
			modifyUserCmd.Flags().StringVar(&FlagQosName, "add_allowed_qos_list", "", "Add a new item to the allowed qos list")
			modifyUserCmd.Flags().StringSliceVar(&FlagPartitions, "delete_allowed_partition", nil, "Delete some specific items from allowed partition list")
			modifyUserCmd.Flags().StringVar(&FlagQosName, "delete_allowed_qos_list", "", "Delete a specific item from allowed qos list")
			modifyUserCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Name of the user being modified")
			modifyUserCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Partition which being modified, if this parameter is not set explicitly, all partitions are modified by default")
			modifyUserCmd.Flags().StringVarP(&FlagAccountName, "account", "A", "", "Set the account used by the user")
			modifyUserCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced operation")
			modifyUserCmd.MarkFlagsMutuallyExclusive("set_allowed_partition", "add_allowed_partition", "delete_allowed_partition")
			modifyUserCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
			if err := modifyUserCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalf("Can't mark 'name' flag required")
			}
		}

		modifyCmd.AddCommand(modifyQosCmd)
		{
			modifyQosCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Name of the qos being modified")
			modifyQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Modify information to describe qos")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 0, "")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", math.MaxUint32, "")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", math.MaxUint32, "")
			modifyQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", uint64(util.InvalidDuration().Seconds), "time in seconds")
			if err := modifyQosCmd.MarkFlagRequired("name"); err != nil {
				return
			}
		}
	}

	/* ---------------------------------------------------- show ---------------------------------------------------- */
	RootCmd.AddCommand(showCmd)
	{
		showCmd.AddCommand(showAccountCmd)
		{
			showAccountCmd.Flags().BoolVarP(&FlagNoHeader, "no-header", "n", false, "no headers on output")
			showAccountCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
				`Specify the output format for the command.
Fields are identified by a percent sign (%) followed by a character. 
Use a dot (.) and a number between % and the format character to specify a minimum width for the field. 
		
Supported format identifiers:
		%n: Name              - Display the name of the account. Optionally, use %.<width>n to specify a fixed width.
		%d: Description       - Display the description of the account.
		%P: AllowedPartition  - Display allowed partitions, separated by commas.
		%Q: DefaultQos        - Display the default Quality of Service (QoS).
		%q: AllowedQosList    - Display a list of allowed QoS, separated by commas.

Example: --format "%.5n %.20d %p" will output account's Name with a minimum width of 5, 
Description with a minimum width of 20, and Partitions.`)
		}

		showCmd.AddCommand(showUserCmd)
		{
			showUserCmd.Flags().StringVarP(&FlagAccountName, "account", "A", "", "The account where the user resides")
		}

		showCmd.AddCommand(showQosCmd)
	}

	/* ---------------------------------------------------- find ---------------------------------------------------- */
	RootCmd.AddCommand(findCmd)
	{
		findCmd.AddCommand(findAccountCmd)
		findCmd.AddCommand(findQosCmd)
		findCmd.AddCommand(findUserCmd)
		{
			findUserCmd.Flags().StringVarP(&FlagAccountName, "account", "A", "", "The account where the user resides")
		}

		findCmd.SetUsageTemplate(`Usage:
  cacctmgr {{.Use}} [name]

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}
`)
	}

	/* --------------------------------------------------- block ---------------------------------------------------- */
	RootCmd.AddCommand(blockCmd)
	{
		blockCmd.AddCommand(blockAccountCmd)
		blockCmd.AddCommand(blockUserCmd)
		{
			blockUserCmd.Flags().StringVarP(&FlagName, "account", "A", "", "The account where the user resides")
			if err := blockUserCmd.MarkFlagRequired("account"); err != nil {
				return
			}
		}

		blockCmd.SetUsageTemplate(`Usage:
  cacctmgr {{.Use}} [name]

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}
`)
	}
	/* -------------------------------------------------- unblock --------------------------------------------------- */
	RootCmd.AddCommand(unblockCmd)
	{
		unblockCmd.AddCommand(unblockAccountCmd)
		unblockCmd.AddCommand(unblockUserCmd)
		{
			unblockUserCmd.Flags().StringVarP(&FlagName, "account", "A", "", "The account where the user resides")
		}

		if err := unblockCmd.MarkFlagRequired("account"); err != nil {
			return
		}

		unblockCmd.SetUsageTemplate(`Usage:
  cacctmgr {{.Use}} [name]

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}
`)
	}
}
