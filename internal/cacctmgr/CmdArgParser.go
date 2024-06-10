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
	FlagAccount protos.AccountInfo
	FlagUser    protos.UserInfo
	FlagQos     protos.QosInfo

	// FlagPartition and FlagSetPartition are different.
	// FlagPartition limits the operation to a specific partition,
	// while the other is the partition to be added or deleted.
	FlagPartition    string
	FlagSetPartition string

	// FlagSetLevel and FlagLevel are different as
	// they have different default values.
	FlagLevel    string
	FlagSetLevel string

	// UserInfo does not have these fields (while AccountInfo does),
	// so we use separate flags for them.
	FlagUserCoordinator bool
	FlagUserDefaultQos  string
	FlagUserPartitions  []string
	FlagUserQosList     []string

	FlagForce          bool
	FlagFull           bool
	FlagConfigFilePath string

	// These flags are implemented,
	// but not added to any cmd!
	FlagNoHeader bool
	FlagFormat   string

	RootCmd = &cobra.Command{
		Use:   "cacctmgr",
		Short: "Manage account, user and QoS tables",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// The PersistentPreRun functions will be inherited and executed by children (sub-commands)
			// if they do not declare their own.
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			userUid = uint32(os.Getuid())
		},
	}

	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	addCmd = &cobra.Command{
		Use:           "add",
		Short:         "Add entity",
		SilenceErrors: true,
		Long:          "",
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
			if err := AddUser(&FlagUser, FlagUserPartitions, FlagLevel, FlagUserCoordinator); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	addQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Add a new QoS",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := AddQos(&FlagQos); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}

	/* --------------------------------------------------- delete --------------------------------------------------- */
	deleteCmd = &cobra.Command{
		Use:           "delete",
		Aliases:       []string{"remove"},
		SilenceErrors: true,
		Short:         "Delete entity",
		Long:          "",
	}
	deleteAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Delete an existing account",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := DeleteAccount(FlagAccount.Name); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	deleteUserCmd = &cobra.Command{
		Use:   "user",
		Short: "Delete an existing user",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := DeleteUser(FlagUser.Name, FlagUser.Account); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	deleteQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Delete an existing QoS",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := DeleteQos(FlagQos.Name); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}

	/* --------------------------------------------------- modify  -------------------------------------------------- */
	modifyCmd = &cobra.Command{
		Use:           "modify",
		Aliases:       []string{"update"},
		SilenceErrors: true,
		Short:         "Modify entity",
		Long:          "",
	}
	modifyAccountCmd = &cobra.Command{
		Use:   "account",
		Short: "Modify account information",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(0)(cmd, args); err != nil {
				return err
			}

			if cmd.Flags().NFlag() < 2 {
				return fmt.Errorf("you must specify at least one modification item")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := util.ErrorSuccess
			if cmd.Flags().Changed("description") { //See if a flag was set by the user
				err = ModifyAccount("description", FlagAccount.Description, FlagAccount.Name, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			//if cmd.Flags().Changed("parent") {
			//	ModifyAccount("parent_account", FlagAccount.ParentAccount, FlagName, protos.ModifyEntityRequest_Overwrite)
			//}
			if cmd.Flags().Changed("set_allowed_partition") {
				err = ModifyAccount("allowed_partition", strings.Join(FlagAccount.AllowedPartitions, ","), FlagAccount.Name, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_partition") {
				err = ModifyAccount("allowed_partition", FlagSetPartition, FlagAccount.Name, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_partition") {
				err = ModifyAccount("allowed_partition", FlagSetPartition, FlagAccount.Name, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set_allowed_qos_list") {
				err = ModifyAccount("allowed_qos_list", strings.Join(FlagAccount.AllowedQosList, ","), FlagAccount.Name, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_qos_list") {
				err = ModifyAccount("allowed_qos_list", FlagQos.Name, FlagAccount.Name, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_qos_list") {
				err = ModifyAccount("allowed_qos_list", FlagQos.Name, FlagAccount.Name, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("default-qos") {
				err = ModifyAccount("default_qos", FlagAccount.DefaultQos, FlagAccount.Name, protos.ModifyEntityRequest_Overwrite)
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
			if err := cobra.ExactArgs(0)(cmd, args); err != nil {
				return err
			}

			if cmd.Flags().NFlag() < 2 {
				return fmt.Errorf("you must specify at least one modification item")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := util.ErrorSuccess
			// Check if a flag was set explicitly
			if cmd.Flags().Changed("admin_level") {
				err = ModifyUser("admin_level", FlagSetLevel, FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set_allowed_partition") {
				err = ModifyUser("allowed_partition", strings.Join(FlagUserPartitions, ","), FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_partition") {
				err = ModifyUser("allowed_partition", FlagSetPartition, FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_partition") {
				err = ModifyUser("allowed_partition", FlagSetPartition, FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set_allowed_qos_list") {
				err = ModifyUser("allowed_qos_list", strings.Join(FlagUserQosList, ","), FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_qos_list") {
				err = ModifyUser("allowed_qos_list", FlagQos.Name, FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_qos_list") {
				err = ModifyUser("allowed_qos_list", FlagQos.Name, FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("default-qos") {
				err = ModifyUser("default_qos", FlagUserDefaultQos, FlagUser.Name, FlagUser.Account, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	modifyQosCmd = &cobra.Command{
		Use:   "qos",
		Short: "Modify QoS information",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(0)(cmd, args); err != nil {
				return err
			}

			if cmd.Flags().NFlag() < 2 {
				return fmt.Errorf("you must specify at least one modification item")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("description") {
				if err := ModifyQos("description", FlagQos.Description, FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("priority") {
				if err := ModifyQos("priority", fmt.Sprint(FlagQos.Priority), FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max_jobs_per_user") {
				if err := ModifyQos("max_jobs_per_user", fmt.Sprint(FlagQos.MaxJobsPerUser), FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max_cpus_per_user") {
				if err := ModifyQos("max_cpus_per_user", fmt.Sprint(FlagQos.MaxCpusPerUser), FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max_time_limit_per_task") {
				if err := ModifyQos("max_time_limit_per_task", fmt.Sprint(FlagQos.MaxTimeLimitPerTask), FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
		},
	}

	/* ---------------------------------------------------- find ---------------------------------------------------- */
	findCmd = &cobra.Command{
		Use:           "show",
		Aliases:       []string{"search", "query", "find"},
		SilenceErrors: true,
		Short:         "Find a specific entity",
		Long:          "",
	}
	findAccountCmd = &cobra.Command{
		Use:     "account",
		Aliases: []string{"accounts"},
		Short:   "Find and display information of account",
		Long:    "",
		Args:    cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				if err := ShowAccounts(); err != util.ErrorSuccess {
					os.Exit(err)
				}
			} else {
				if err := FindAccount(args[0]); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
		},
	}
	findUserCmd = &cobra.Command{
		Use:     "user",
		Aliases: []string{"users"},
		Short:   "Find and display information of user",
		Long:    "",
		Args:    cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				if err := ShowUser("", FlagUser.Account); err != util.ErrorSuccess {
					os.Exit(err)
				}
			} else {
				if err := ShowUser(args[0], FlagUser.Account); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
		},
	}
	findQosCmd = &cobra.Command{
		Use:   "qos [flags] name",
		Short: "Find and display information of a specific QoS",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				if err := ShowQos(""); err != util.ErrorSuccess {
					os.Exit(err)
				}
			} else {
				if err := ShowQos(args[0]); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
		},
	}
	/* --------------------------------------------------- block ---------------------------------------------------- */
	blockCmd = &cobra.Command{
		Use:           "block",
		SilenceErrors: true,
		Short:         "Block the entity so that it cannot be used",
		Long:          "",
	}
	blockAccountCmd = &cobra.Command{
		Use:   "account [flags] name",
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
		Use:   "user [flags] name",
		Short: "Block a user under an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := BlockAccountOrUser(args[0], protos.EntityType_User, FlagUser.Account); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	/* -------------------------------------------------- unblock --------------------------------------------------- */
	unblockCmd = &cobra.Command{
		Use:           "unblock",
		SilenceErrors: true,
		Short:         "Unblock the entity",
		Long:          "",
	}
	unblockAccountCmd = &cobra.Command{
		Use:   "account [flags] name",
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
		Use:   "user [flags] name",
		Short: "Unblock a user under an account",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := UnblockAccountOrUser(args[0], protos.EntityType_User, FlagUser.Account); err != util.ErrorSuccess {
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
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")

	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	RootCmd.AddCommand(addCmd)
	{
		addCmd.AddCommand(addAccountCmd)
		{
			addAccountCmd.Flags().StringVarP(&FlagAccount.Name, "name", "N", "", "Set the name of the account")
			addAccountCmd.Flags().StringVarP(&FlagAccount.Description, "description", "D", "", "Set the description of the account")
			addAccountCmd.Flags().StringVarP(&FlagAccount.ParentAccount, "parent", "P", "", "Set the parent account of the account")
			addAccountCmd.Flags().StringSliceVarP(&FlagAccount.AllowedPartitions, "partition", "p", nil, "Set allowed partitions of the account (comma seperated list)")
			addAccountCmd.Flags().StringVarP(&FlagAccount.DefaultQos, "default-qos", "Q", "", "Set default QoS of the account")
			addAccountCmd.Flags().StringSliceVarP(&FlagAccount.AllowedQosList, "qos", "q", nil, "Set allowed QoS of the account (comma seperated list)")
			if err := addAccountCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}

		addCmd.AddCommand(addUserCmd)
		{
			addUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "Set the name of the user")
			addUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Set the account of the user")
			addUserCmd.Flags().StringSliceVarP(&FlagUserPartitions, "partition", "p", nil, "Set allowed partitions of the user (comma seperated list)")
			addUserCmd.Flags().StringVarP(&FlagLevel, "level", "L", "none", "Set admin level (none/operator) of the user")
			addUserCmd.Flags().BoolVarP(&FlagUserCoordinator, "coordinator", "c", false, "Set the user as a coordinator of the account")
			if err := addUserCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
			if err := addUserCmd.MarkFlagRequired("account"); err != nil {
				log.Fatalln("Can't mark 'account' flag required")
			}
		}

		addCmd.AddCommand(addQosCmd)
		{
			addQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "Set the name of the QoS")
			addQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Set the description of the QoS")
			addQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 1000, "Set job priority of the QoS")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", math.MaxUint32, "Set the maximum number of jobs per user")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", math.MaxUint32, "Set the maximum number of CPUs per user")
			addQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", uint64(util.InvalidDuration().Seconds), "Set the maximum time limit per job (in seconds)")
			if err := addQosCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}
	}

	/* --------------------------------------------------- remove --------------------------------------------------- */
	RootCmd.AddCommand(deleteCmd)
	{
		deleteCmd.AddCommand(deleteAccountCmd)
		{
			deleteAccountCmd.Flags().StringVarP(&FlagAccount.Name, "name", "N", "", "Remove account with this name")
			if err := deleteAccountCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}

		deleteCmd.AddCommand(deleteQosCmd)
		{
			deleteQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "Remove QoS with this name")
			if err := deleteQosCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}

		deleteCmd.AddCommand(deleteUserCmd)
		{
			deleteUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "Remove user with this name")
			deleteUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Remove user from this account")
			if err := deleteUserCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}
	}

	/* --------------------------------------------------- modify  -------------------------------------------------- */
	RootCmd.AddCommand(modifyCmd)
	{
		modifyCmd.AddCommand(modifyAccountCmd)
		{
			// Where flags
			modifyAccountCmd.Flags().StringVarP(&FlagAccount.Name, "name", "N", "", "Specify the name of the account to be modified")

			// Set flags
			modifyAccountCmd.Flags().StringVarP(&FlagAccount.Description, "description", "D", "", "Set the description of the account")
			// modifyAccountCmd.Flags().StringVarP(&FlagAccount.ParentAccount, "parent", "P", "", "Modify parent account")
			modifyAccountCmd.Flags().StringVarP(&FlagAccount.DefaultQos, "default-qos", "Q", "", "Set default QoS of the account")

			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedPartitions, "set_allowed_partition", nil, "Overwrite allowed partitions of the account (comma seperated list)")
			modifyAccountCmd.Flags().StringVar(&FlagSetPartition, "add_allowed_partition", "", "Add a single partition to allowed partition list")
			modifyAccountCmd.Flags().StringVar(&FlagSetPartition, "delete_allowed_partition", "", "Delete a single partition from allowed partition list")

			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "set_allowed_qos_list", nil, "Overwrite allowed QoS list of the user (comma seperated list)")
			modifyAccountCmd.Flags().StringVar(&FlagQos.Name, "add_allowed_qos_list", "", "Add a single QoS to allowed QoS list")
			modifyAccountCmd.Flags().StringVar(&FlagQos.Name, "delete_allowed_qos_list", "", "Delete a single QoS from allowed QoS list")

			// Other flags
			modifyAccountCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced to operate")

			// Rules
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_partition", "add_allowed_partition", "delete_allowed_partition")
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
			if err := modifyAccountCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}

		modifyCmd.AddCommand(modifyUserCmd)
		{
			// Where flags
			modifyUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "Specify the name of the user to be modified")
			modifyUserCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Specify the partition used (if not set, all partitions are modified)")
			modifyUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Specify the account used (if not set, default account is used)")

			// Set flags
			modifyUserCmd.Flags().StringVarP(&FlagUserDefaultQos, "default-qos", "Q", "", "Set default QoS of the user")
			modifyUserCmd.Flags().StringVarP(&FlagSetLevel, "admin_level", "L", "", "Set admin level (none/operator/admin) of the user")

			modifyUserCmd.Flags().StringSliceVar(&FlagUserPartitions, "set_allowed_partition", nil, "Overwrite allowed partitions of the user (comma seperated list)")
			modifyUserCmd.Flags().StringVar(&FlagSetPartition, "add_allowed_partition", "", "Add a single partition to allowed partition list")
			modifyUserCmd.Flags().StringVar(&FlagSetPartition, "delete_allowed_partition", "", "Delete a single partition to allowed partition list")

			modifyUserCmd.Flags().StringSliceVar(&FlagUserQosList, "set_allowed_qos_list", nil, "Overwrite allowed QoS list of the user (comma seperated list)")
			modifyUserCmd.Flags().StringVar(&FlagQos.Name, "add_allowed_qos_list", "", "Add a single QoS to allowed QoS list")
			modifyUserCmd.Flags().StringVar(&FlagQos.Name, "delete_allowed_qos_list", "", "Delete a single QoS from allowed QoS list")

			// Other flags
			modifyUserCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced operation")

			// Rules
			modifyUserCmd.MarkFlagsMutuallyExclusive("partition", "set_allowed_partition", "add_allowed_partition", "delete_allowed_partition")
			modifyUserCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
			if err := modifyUserCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}

		modifyCmd.AddCommand(modifyQosCmd)
		{
			// Where flags
			modifyQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "Specify the name of the QoS to be modified")

			// Set flags
			modifyQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Set description of the QoS")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 0, "Set job priority of the QoS")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", math.MaxUint32, "Set the maximum number of jobs per user")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", math.MaxUint32, "Set the maximum number of CPUs per user")
			modifyQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", uint64(util.InvalidDuration().Seconds), "Set the maximum time limit per job (in seconds)")

			// Rules
			if err := modifyQosCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}
	}

	/* ---------------------------------------------------- find ---------------------------------------------------- */
	RootCmd.AddCommand(findCmd)
	{
		findCmd.AddCommand(findAccountCmd)
		findCmd.AddCommand(findQosCmd)
		findCmd.AddCommand(findUserCmd)
		{
			findUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Display the user under the specified account")
		}

		findCmd.PersistentFlags().BoolVarP(&FlagFull, "full", "F", false, "Display full information (If not set, only display 30 characters per cell)")
	}

	/* --------------------------------------------------- block ---------------------------------------------------- */
	RootCmd.AddCommand(blockCmd)
	{
		blockCmd.AddCommand(blockAccountCmd)
		blockCmd.AddCommand(blockUserCmd)
		{
			blockUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Block the user under the specified account")
			if err := blockUserCmd.MarkFlagRequired("account"); err != nil {
				log.Fatalln("Can't mark 'account' flag required")
			}
		}
	}

	/* -------------------------------------------------- unblock --------------------------------------------------- */
	RootCmd.AddCommand(unblockCmd)
	{
		unblockCmd.AddCommand(unblockAccountCmd)
		unblockCmd.AddCommand(unblockUserCmd)
		{
			unblockUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Unblock the user under the specified account")
			if err := unblockUserCmd.MarkFlagRequired("account"); err != nil {
				log.Fatalln("Can't mark 'account' flag required")
			}
		}
	}
}
