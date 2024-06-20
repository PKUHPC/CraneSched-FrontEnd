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
			if err := AddUser(&FlagUser, FlagPartitions, FlagLevel, FlagCoordinate); err != util.ErrorSuccess {
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

	/* --------------------------------------------------- remove --------------------------------------------------- */
	removeCmd = &cobra.Command{
		Use:           "delete",
		Aliases:       []string{"remove"},
		SilenceErrors: true,
		Short:         "Delete entity",
		Long:          "",
	}
	removeAccountCmd = &cobra.Command{
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
	removeUserCmd = &cobra.Command{
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
	removeQosCmd = &cobra.Command{
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
				err = ModifyAccount("allowed_qos_list", FlagQosName, FlagName, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_qos_list") {
				err = ModifyAccount("allowed_qos_list", FlagQosName, FlagName, protos.ModifyEntityRequest_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("default-qos") {
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
			if cmd.Flags().Changed("admin_level") { //See if a flag was set by the user
				err = ModifyUser("admin_level", FlagLevel, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set_allowed_partition") {
				err = ModifyUser("allowed_partition", strings.Join(FlagPartitions, ","), FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Overwrite)
			} else if cmd.Flags().Changed("add_allowed_partition") {
				err = ModifyUser("allowed_partition", FlagPartition, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Add)
			} else if cmd.Flags().Changed("delete_allowed_partition") {
				err = ModifyUser("allowed_partition", FlagPartition, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Delete)
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
			if cmd.Flags().Changed("default-qos") {
				err = ModifyUser("default_qos", FlagSetDefaultQos, FlagName, FlagAccountName, FlagPartition, protos.ModifyEntityRequest_Overwrite)
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
				if err := ShowUser("", FlagAccountName); err != util.ErrorSuccess {
					os.Exit(err)
				}
			} else {
				if err := ShowUser(args[0], FlagAccountName); err != util.ErrorSuccess {
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
			if err := BlockAccountOrUser(args[0], protos.EntityType_User, FlagName); err != util.ErrorSuccess {
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
			if err := UnblockAccountOrUser(args[0], protos.EntityType_User, FlagName); err != util.ErrorSuccess {
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
				return
			}
		}

		addCmd.AddCommand(addUserCmd)
		{
			addUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "Set the name of the user")
			addUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Set the account of the user")
			addUserCmd.Flags().StringSliceVarP(&FlagPartitions, "partition", "p", nil, "Set allowed partitions of the user (comma seperated list)")
			addUserCmd.Flags().StringVarP(&FlagLevel, "level", "L", "none", "Set admin level (none/operator) of the user")
			addUserCmd.Flags().BoolVarP(&FlagCoordinate, "coordinate", "c", false, "Set the user as a coordinator of the account")
			if err := addUserCmd.MarkFlagRequired("name"); err != nil {
				return
			}
			if err := addUserCmd.MarkFlagRequired("account"); err != nil {
				return
			}
		}

		addCmd.AddCommand(addQosCmd)
		{
			addQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "Set the name of the QoS")
			addQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Set the description of the QoS")
			addQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 0, "Set job priority of the QoS")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", math.MaxUint32, "Set the maximum number of jobs per user")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", math.MaxUint32, "Set the maximum number of CPUs per user")
			addQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", uint64(util.InvalidDuration().Seconds), "Set the maximum time limit per job (in seconds)")
			if err := addQosCmd.MarkFlagRequired("name"); err != nil {
				return
			}
		}
	}

	/* --------------------------------------------------- remove --------------------------------------------------- */
	RootCmd.AddCommand(removeCmd)
	{
		removeCmd.AddCommand(removeAccountCmd)
		{
			removeAccountCmd.Flags().StringVarP(&FlagAccount.Name, "name", "N", "", "Remove account with this name")
			if err := removeAccountCmd.MarkFlagRequired("name"); err != nil {
				return
			}
		}

		removeCmd.AddCommand(removeQosCmd)
		{
			removeQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "Remove QoS with this name")
			if err := removeQosCmd.MarkFlagRequired("name"); err != nil {
				return
			}
		}

		removeCmd.AddCommand(removeUserCmd)
		{
			removeUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "Remove user with this name")
			removeUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Remove user from this account")
			if err := removeUserCmd.MarkFlagRequired("name"); err != nil {
				return
			}
		}
	}

	/* --------------------------------------------------- modify  -------------------------------------------------- */
	RootCmd.AddCommand(modifyCmd)
	{
		modifyCmd.AddCommand(modifyAccountCmd)
		{
			// Where flags
			modifyAccountCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Specify the name of the account to be modified")

			// Set flags
			modifyAccountCmd.Flags().StringVarP(&FlagAccount.Description, "description", "D", "", "Set the description of the account")
			// modifyAccountCmd.Flags().StringVarP(&FlagAccount.ParentAccount, "parent", "P", "", "Modify parent account")
			modifyAccountCmd.Flags().StringVarP(&FlagAccount.DefaultQos, "default-qos", "Q", "", "Set default QoS of the account")

			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedPartitions, "set_allowed_partition", nil, "Overwrite allowed partitions of the account (comma seperated list)")
			modifyAccountCmd.Flags().StringVar(&FlagPartition, "add_allowed_partition", "", "Add a single partition to allowed partition list")
			modifyAccountCmd.Flags().StringVar(&FlagPartition, "delete_allowed_partition", "", "Delete a single partition from allowed partition list")

			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "set_allowed_qos_list", nil, "Overwrite allowed QoS list of the user (comma seperated list)")
			modifyAccountCmd.Flags().StringVar(&FlagQosName, "add_allowed_qos_list", "", "Add a single QoS to allowed QoS list")
			modifyAccountCmd.Flags().StringVar(&FlagQosName, "delete_allowed_qos_list", "", "Delete a single QoS from allowed QoS list")

			// Other flags
			modifyAccountCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced to operate")

			// Rules
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_partition", "add_allowed_partition", "delete_allowed_partition")
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
			if err := modifyAccountCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalf("Can't mark 'name' flag required")
			}
		}

		modifyCmd.AddCommand(modifyUserCmd)
		{
			// Where flags
			modifyUserCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Specify the name of the user to be modified")
			modifyUserCmd.Flags().StringVarP(&FlagPartition, "partition", "p", "", "Specify the partition used (if not set, all partitions are modified)")
			modifyUserCmd.Flags().StringVarP(&FlagAccountName, "account", "A", "", "Specify the account used (if not set, default account is used)")

			// Set flags
			modifyUserCmd.Flags().StringVarP(&FlagSetDefaultQos, "default-qos", "Q", "", "Set default QoS of the user")
			modifyUserCmd.Flags().StringVarP(&FlagLevel, "admin_level", "L", "", "Set admin level (none/operator/admin) of the user")

			modifyUserCmd.Flags().StringSliceVar(&FlagPartitions, "set_allowed_partition", nil, "Overwrite allowed partitions of the user (comma seperated list)")
			modifyUserCmd.Flags().StringVar(&FlagPartition, "add_allowed_partition", "", "Add a single partition to allowed partition list")
			modifyUserCmd.Flags().StringVar(&FlagPartition, "delete_allowed_partition", "", "Delete a single partition to allowed partition list")

			modifyUserCmd.Flags().StringSliceVar(&FlagAllowedQosList, "set_allowed_qos_list", nil, "Overwrite allowed QoS list of the user (comma seperated list)")
			modifyUserCmd.Flags().StringVar(&FlagQosName, "add_allowed_qos_list", "", "Add a single QoS to allowed QoS list")
			modifyUserCmd.Flags().StringVar(&FlagQosName, "delete_allowed_qos_list", "", "Delete a single QoS from allowed QoS list")

			// Other flags
			modifyUserCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced operation")

			// Rules
			modifyUserCmd.MarkFlagsMutuallyExclusive("set_allowed_partition", "add_allowed_partition", "delete_allowed_partition")
			modifyUserCmd.MarkFlagsMutuallyExclusive("set_allowed_qos_list", "add_allowed_qos_list", "delete_allowed_qos_list")
			if err := modifyUserCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalf("Can't mark 'name' flag required")
			}
		}

		modifyCmd.AddCommand(modifyQosCmd)
		{
			// Where flags
			modifyQosCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Specify the name of the QoS to be modified")

			// Set flags
			modifyQosCmd.Flags().StringVarP(&FlagQos.Description, "description", "D", "", "Set description of the QoS")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 0, "Set job priority of the QoS")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max_jobs_per_user", "J", math.MaxUint32, "Set the maximum number of jobs per user")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max_cpus_per_user", "c", math.MaxUint32, "Set the maximum number of CPUs per user")
			modifyQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max_time_limit_per_task", "T", uint64(util.InvalidDuration().Seconds), "Set the maximum time limit per job (in seconds)")

			// Rules
			if err := modifyQosCmd.MarkFlagRequired("name"); err != nil {
				return
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
			findUserCmd.Flags().StringVarP(&FlagAccountName, "account", "A", "", "Display the user under the specified account")
		}
	}

	/* --------------------------------------------------- block ---------------------------------------------------- */
	RootCmd.AddCommand(blockCmd)
	{
		blockCmd.AddCommand(blockAccountCmd)
		blockCmd.AddCommand(blockUserCmd)
		{
			blockUserCmd.Flags().StringVarP(&FlagName, "account", "A", "", "Block the user under the specified account")
			if err := blockUserCmd.MarkFlagRequired("account"); err != nil {
				return
			}
		}
	}

	/* -------------------------------------------------- unblock --------------------------------------------------- */
	RootCmd.AddCommand(unblockCmd)
	{
		unblockCmd.AddCommand(unblockAccountCmd)
		unblockCmd.AddCommand(unblockUserCmd)
		{
			unblockUserCmd.Flags().StringVarP(&FlagName, "account", "A", "", "Unblock the user under the specified account")
			if err := unblockUserCmd.MarkFlagRequired("account"); err != nil {
				return
			}
		}
	}
}
