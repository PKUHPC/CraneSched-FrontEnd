/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
	FlagLevel             string
	FlagSetLevel          string
	FlagSetDefaultAccount string

	// UserInfo does not have these fields (while AccountInfo does),
	// so we use separate flags for them.
	FlagUserCoordinator bool
	FlagUserDefaultQos  string
	FlagUserPartitions  []string
	FlagUserQosList     []string

	FlagForce          bool
	FlagFull           bool
	FlagJson           bool
	FlagConfigFilePath string

	// These flags are implemented,
	// but not added to any cmd!
	FlagNoHeader bool
	FlagFormat   string

	RootCmd = &cobra.Command{
		Use:     "cacctmgr",
		Short:   "Manage account, user and QoS tables",
		Long:    "",
		Version: util.Version(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// The PersistentPreRun functions will be inherited and executed by children (sub-commands)
			// if they do not declare their own.
			util.DetectNetworkProxy()
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
				err = ModifyAccount(protos.ModifyField_Description, FlagAccount.Description, FlagAccount.Name, protos.OperationType_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			//if cmd.Flags().Changed("parent") {
			//	ModifyAccount("parent_account", FlagAccount.ParentAccount, FlagName, protos.ModifyEntityRequest_Overwrite)
			//}
			if cmd.Flags().Changed("set-allowed-partition") {
				err = ModifyAccount(protos.ModifyField_Partition, strings.Join(FlagAccount.AllowedPartitions, ","), FlagAccount.Name, protos.OperationType_Overwrite)
			} else if cmd.Flags().Changed("add-allowed-partition") {
				err = ModifyAccount(protos.ModifyField_Partition, FlagSetPartition, FlagAccount.Name, protos.OperationType_Add)
			} else if cmd.Flags().Changed("delete-allowed-partition") {
				err = ModifyAccount(protos.ModifyField_Partition, FlagSetPartition, FlagAccount.Name, protos.OperationType_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set-allowed-qos-list") {
				err = ModifyAccount(protos.ModifyField_Qos, strings.Join(FlagAccount.AllowedQosList, ","), FlagAccount.Name, protos.OperationType_Overwrite)
			} else if cmd.Flags().Changed("add-allowed-qos-list") {
				err = ModifyAccount(protos.ModifyField_Qos, FlagQos.Name, FlagAccount.Name, protos.OperationType_Add)
			} else if cmd.Flags().Changed("delete-allowed-qos-list") {
				err = ModifyAccount(protos.ModifyField_Qos, FlagQos.Name, FlagAccount.Name, protos.OperationType_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("default-qos") {
				err = ModifyAccount(protos.ModifyField_DefaultQos, FlagAccount.DefaultQos, FlagAccount.Name, protos.OperationType_Overwrite)
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
			if cmd.Flags().Changed("admin-level") {
				err = ModifyUser(protos.ModifyField_AdminLevel, FlagSetLevel, FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}

			if cmd.Flags().Changed("default-account") {
				err = ModifyUser(protos.ModifyField_DefaultAccount, FlagSetDefaultAccount, FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Overwrite)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}

			if cmd.Flags().Changed("set-allowed-partition") {
				err = ModifyUser(protos.ModifyField_Partition, strings.Join(FlagUserPartitions, ","), FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Overwrite)
			} else if cmd.Flags().Changed("add-allowed-partition") {
				err = ModifyUser(protos.ModifyField_Partition, FlagSetPartition, FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Add)
			} else if cmd.Flags().Changed("delete-allowed-partition") {
				err = ModifyUser(protos.ModifyField_Partition, FlagSetPartition, FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("set-allowed-qos-list") {
				err = ModifyUser(protos.ModifyField_Qos, strings.Join(FlagUserQosList, ","), FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Overwrite)
			} else if cmd.Flags().Changed("add-allowed-qos-list") {
				err = ModifyUser(protos.ModifyField_Qos, FlagQos.Name, FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Add)
			} else if cmd.Flags().Changed("delete-allowed-qos-list") {
				err = ModifyUser(protos.ModifyField_Qos, FlagQos.Name, FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Delete)
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
			if cmd.Flags().Changed("default-qos") {
				err = ModifyUser(protos.ModifyField_DefaultQos, FlagUserDefaultQos, FlagUser.Name, FlagUser.Account, FlagPartition, protos.OperationType_Overwrite)
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
				if err := ModifyQos(protos.ModifyField_Description, FlagQos.Description, FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("priority") {
				if err := ModifyQos(protos.ModifyField_Priority, fmt.Sprint(FlagQos.Priority), FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max-jobs-per-user") {
				if err := ModifyQos(protos.ModifyField_MaxJobsPerUser, fmt.Sprint(FlagQos.MaxJobsPerUser), FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max-cpus-per-user") {
				if err := ModifyQos(protos.ModifyField_MaxCpusPerUser, fmt.Sprint(FlagQos.MaxCpusPerUser), FlagQos.Name); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("max-time-limit-per-task") {
				if err := ModifyQos(protos.ModifyField_MaxTimeLimitPerTask, fmt.Sprint(FlagQos.MaxTimeLimitPerTask), FlagQos.Name); err != util.ErrorSuccess {
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
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().BoolVar(&FlagJson, "json",
		false, "Output in JSON format")

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
			addQosCmd.Flags().Uint32VarP(&FlagQos.Priority, "priority", "P", 0, "Set job priority of the QoS")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max-jobs-per-user", "J", math.MaxUint32, "Set the maximum number of jobs per user")
			addQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max-cpus-per-user", "c", math.MaxUint32, "Set the maximum number of CPUs per user")
			addQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max-time-limit-per-task", "T", util.MaxJobTimeLimit, "Set the maximum time limit per job (in seconds)")
			if err := addQosCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
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
				log.Fatalln("Can't mark 'name' flag required")
			}
		}

		removeCmd.AddCommand(removeQosCmd)
		{
			removeQosCmd.Flags().StringVarP(&FlagQos.Name, "name", "N", "", "Remove QoS with this name")
			if err := removeQosCmd.MarkFlagRequired("name"); err != nil {
				log.Fatalln("Can't mark 'name' flag required")
			}
		}

		removeCmd.AddCommand(removeUserCmd)
		{
			removeUserCmd.Flags().StringVarP(&FlagUser.Name, "name", "N", "", "Remove user with this name")
			removeUserCmd.Flags().StringVarP(&FlagUser.Account, "account", "A", "", "Remove user from this account")
			if err := removeUserCmd.MarkFlagRequired("name"); err != nil {
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

			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedPartitions, "set-allowed-partition", nil, "Overwrite allowed partitions of the account (comma seperated list)")
			modifyAccountCmd.Flags().StringVar(&FlagSetPartition, "add-allowed-partition", "", "Add a single partition to allowed partition list")
			modifyAccountCmd.Flags().StringVar(&FlagSetPartition, "delete-allowed-partition", "", "Delete a single partition from allowed partition list")

			modifyAccountCmd.Flags().StringSliceVar(&FlagAccount.AllowedQosList, "set-allowed-qos-list", nil, "Overwrite allowed QoS list of the user (comma seperated list)")
			modifyAccountCmd.Flags().StringVar(&FlagQos.Name, "add-allowed-qos-list", "", "Add a single QoS to allowed QoS list")
			modifyAccountCmd.Flags().StringVar(&FlagQos.Name, "delete-allowed-qos-list", "", "Delete a single QoS from allowed QoS list")

			// Other flags
			modifyAccountCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced to operate")

			// Rules
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set-allowed-partition", "add-allowed-partition", "delete-allowed-partition")
			modifyAccountCmd.MarkFlagsMutuallyExclusive("set-allowed-qos-list", "add-allowed-qos-list", "delete-allowed-qos-list")
			modifyAccountCmd.MarkFlagsOneRequired("set-allowed-partition", "add-allowed-partition", "delete-allowed-partition",
				"set-allowed-qos-list", "add-allowed-qos-list", "delete-allowed-qos-list", "description", "default-qos")
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
			modifyUserCmd.Flags().StringVarP(&FlagSetLevel, "admin-level", "L", "", "Set admin level (none/operator/admin) of the user")
			modifyUserCmd.Flags().StringVarP(&FlagSetDefaultAccount, "default-account", "D", "", "Modify default account of the user")

			modifyUserCmd.Flags().StringSliceVar(&FlagUserPartitions, "set-allowed-partition", nil, "Overwrite allowed partitions of the user (comma seperated list)")
			modifyUserCmd.Flags().StringVar(&FlagSetPartition, "add-allowed-partition", "", "Add a single partition to allowed partition list")
			modifyUserCmd.Flags().StringVar(&FlagSetPartition, "delete-allowed-partition", "", "Delete a single partition to allowed partition list")

			modifyUserCmd.Flags().StringSliceVar(&FlagUserQosList, "set-allowed-qos-list", nil, "Overwrite allowed QoS list of the user (comma seperated list)")
			modifyUserCmd.Flags().StringVar(&FlagQos.Name, "add-allowed-qos-list", "", "Add a single QoS to allowed QoS list")
			modifyUserCmd.Flags().StringVar(&FlagQos.Name, "delete-allowed-qos-list", "", "Delete a single QoS from allowed QoS list")

			// Other flags
			modifyUserCmd.Flags().BoolVarP(&FlagForce, "force", "F", false, "Forced operation")

			// Rules
			modifyUserCmd.MarkFlagsMutuallyExclusive("partition", "set-allowed-partition", "add-allowed-partition", "delete-allowed-partition")
			modifyUserCmd.MarkFlagsMutuallyExclusive("set-allowed-qos-list", "add-allowed-qos-list", "delete-allowed-qos-list")
			modifyUserCmd.MarkFlagsOneRequired("set-allowed-partition", "add-allowed-partition", "delete-allowed-partition",
				"set-allowed-qos-list", "add-allowed-qos-list", "delete-allowed-qos-list", "default-qos", "admin-level", "default-account")
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
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxJobsPerUser, "max-jobs-per-user", "J", math.MaxUint32, "Set the maximum number of jobs per user")
			modifyQosCmd.Flags().Uint32VarP(&FlagQos.MaxCpusPerUser, "max-cpus-per-user", "c", math.MaxUint32, "Set the maximum number of CPUs per user")
			modifyQosCmd.Flags().Uint64VarP(&FlagQos.MaxTimeLimitPerTask, "max-time-limit-per-task", "T", util.MaxJobTimeLimit, "Set the maximum time limit per job (in seconds)")

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
