/**
 * Copyright (c) 2025 Peking University and Peking University
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
	"CraneFrontEnd/internal/util"
	"fmt"
)

func showHelp() {
	help := `Crane Account Manager (cacctmgr) - version ` + util.Version() + `
  
  USAGE: cacctmgr <ACTION> <ENTITY> [OPTIONS]
  
  ACTIONS:
	add       - Create a new account, user, or QoS
	delete    - Remove an account, user, or QoS
	block     - Block an account or user
	unblock   - Unblock an account or user
	modify    - Change attributes of an account, user, or QoS
	show      - Display basic information
	reset     - Reset user certificate
  
  ENTITIES:
	account   - User account in the system
	user      - Individual user
	qos       - Quality of Service settings
  
  COMMANDS:
	add account <name> [Description=<desc>] [Parent=<parent>] [DefaultQos=<qos>] 
			[Partition=<part1,part2,...>] [QosList=<qos1,qos2,...>] [Name=<name1,name2,...>]
    Create a new account with the specified attributes.
    Parameter details:
      Description=<desc>         Description of the account
      Parent=<parent>            Name of the parent account
      DefaultQos=<qos>           Default QoS for the account
      Partition=<part1,part2,...>  Allowed partitions (comma-separated)
      QosList=<qos1,qos2,...>    Allowed QoS list (comma-separated)
      Name=<name1,name2,...>    List of account name for batch creation
  
	delete account <name>
    Remove the specified account(s) from the system.
    Parameter details:
      Name=<name1,name2,...>    Name of accounts to delete (comma-separated)
  
	show account [Name=<name1,name2,...>]
    Display information about accounts.
    Parameter details:
      Name=<name1,name2,...>    Show only these accounts (comma-separated)
      (If not specified, all accounts will be displayed)
  
	add user <name> Account=<account> [Coordinator=true|false] [Level=<level>] 
		[Partition=<part1,part2,...>] [Name=<name1,name2,...>]
    Create a new user and associate with an account.
    Parameter details:
      Account=<account>          Account the user belongs to (required)
      Coordinator=true|false     Whether the user is a coordinator for the account
      Level=<level>              User admin level
      Partition=<part1,part2,...>  Allowed partitions (comma-separated)
      Name=<name1,name2,...>    List of user name for batch creation
  
	delete user <name> [Account=<account>] [Name=<name1,name2,...>]
    Remove a user from the system or from a specific account.
    Parameter details:
      Account=<account>          Specify the account (optional)
      Name=<name1,name2,...>    Name of users to delete (comma-separated)
  
	show user [Accounts=<account>] [Name=<name1,name2,...>]
    Display information about users.
    Parameter details:
      Accounts=<account>         Show users of this account only
      Name=<name1,name2,...>    Show only these users (comma-separated)
      (If not specified, all users will be displayed)
  
	block account <name> [Account=<account>]
    Block the specified account from submitting jobs.
    Parameter details:
      name                      Name of the account to block
      Account=<account>         Specify the account context (optional)
  
	block user <name> [Account=<account>]
    Block the specified user from submitting jobs.
    Parameter details:
      name                      Name of the user to block
      Account=<account>         Specify the account context (optional)
  
	unblock account <name> [Account=<account>]
    Unblock the specified account to allow job submission.
    Parameter details:
      name                      Name of the account to unblock
      Account=<account>         Specify the account context (optional)
  
	unblock user <name> [Account=<account>]
    Unblock the specified user to allow job submission.
    Parameter details:
      name                      Name of the user to unblock
      Account=<account>         Specify the account context (optional)

	reset <name>
	Reset the specified user's certificate.
	Parameter details:
      name 					    Name of the user to reset
      (if name is 'all', all users will be reset.)
  
	add qos <name> [Description=<desc>] [Priority=<priority>] 
		[MaxJobsPerUser=<num>] [MaxCpusPerUser=<num>] [MaxTimeLimitPerTask=<seconds>]
    Create a new QoS with the specified attributes.
    Parameter details:
      Description=<desc>         Description of the QoS
      Priority=<priority>        Priority (higher value means higher priority)
      MaxJobsPerUser=<num>       Maximum number of jobs per user
      MaxCpusPerUser=<num>       Maximum number of CPUs per user
      MaxTimeLimitPerTask=<seconds>  Maximum run time per task in seconds
      Name=<name1,name2,...>    List of QoS name for batch creation
  
	delete qos <name> [Name=<name1,name2,...>]
    Remove the specified QoS from the system.
    Parameter details:
      Name=<name1,name2,...>    Name of QoS to delete (comma-separated)
  
	show qos <name> [Name=<name1,name2,...>]
    Display information about QoS.
    Parameter details:
      Name=<name1,name2,...>    Show only these QoS (comma-separated)
      (If not specified, all QoS will be displayed)

  modify <entity> where [OPTIONS] set [OPTIONS]
    Modify attributes of an existing account, user, or QoS.
    Account options:
      where Name=<account>         Specify the account to modify
      set Description=<desc>       Set account description
      set DefaultQos=<qos>         Set default QoS
      set AllowedPartition+=<part1,part2,...>   Add allowed partitions
      set AllowedQos+=<qos1,qos2,...>          Add allowed QoS
      set AllowedPartition-=<part1,part2,...> Delete allowed partitions
      set AllowedQos-=<qos1,qos2,...>        Delete allowed QoS
      set AllowedPartition=<part1,part2,...>      Set allowed partitions directly
      set AllowedQos=<qos1,qos2,...>              Set allowed QoS directly
    User options:
      where Name=<user> [Account=<account>] [Partition=<part1,part2,...>]
      set AdminLevel=<level>         Set user admin level
      set DefaultAccount=<account>   Set default account
      set DefaultQos=<qos>           Set default QoS
      set AllowedPartition+=...    Add allowed partitions
      set AllowedQos+=...          Add allowed QoS
      set AllowedPartition-=... Delete allowed partitions
      set AllowedQos-=...       Delete allowed QoS
      set AllowedPartition=...       Set allowed partitions directly
      set AllowedQos=...             Set allowed QoS directly
    QoS options:
      where Name=<qos>
      set Description=<desc>         Set description
      set MaxCpusPerUser=<num>        Set max CPUs per user
      set MaxJobsPerUser=<num> Set max jobs per user
      set MaxTimeLimitPerTask=<sec>  Set max time per task (seconds)
      set Priority=<priority>        Set priority

  GLOBAL OPTIONS:
	--help, -h     Display this help message
	--config, -C   Specify config file path (default: /etc/crane/config.yaml)
	--json, -J     Format output as JSON
	--version, -v  Display program version
	--force, -f    Force operation without confirmation

  NOTE: Parameters in [] are optional. Parameters in <> should be replaced with actual values.
  `
	fmt.Println(help)
}
