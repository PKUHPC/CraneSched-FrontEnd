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
  
  ENTITIES:
	account   - User account in the system
	user      - Individual user
	qos       - Quality of Service settings
  
  COMMANDS:
	add account <name> [description=<desc>] [parent=<parent>] [defaultQos=<qos>] 
			[partition=<part1,part2,...>] [qosList=<qos1,qos2,...>] [name=<name1,name2,...>]
    Create a new account with the specified attributes.
    Parameter details:
      description=<desc>         Description of the account
      parent=<parent>            Name of the parent account
      defaultQos=<qos>           Default QoS for the account
      partition=<part1,part2,...>  Allowed partitions (comma-separated)
      qosList=<qos1,qos2,...>    Allowed QoS list (comma-separated)
      name=<name1,name2,...>    List of account name for batch creation
  
	delete account <name>
    Remove the specified account(s) from the system.
    Parameter details:
      name=<name1,name2,...>    Name of accounts to delete (comma-separated)
  
	show account [name=<name1,name2,...>]
    Display information about accounts.
    Parameter details:
      name=<name1,name2,...>    Show only these accounts (comma-separated)
      (If not specified, all accounts will be displayed)
  
	add user <name> account=<account> [coordinator=true|false] [level=<level>] 
		[partition=<part1,part2,...>] [name=<name1,name2,...>]
    Create a new user and associate with an account.
    Parameter details:
      account=<account>          Account the user belongs to (required)
      coordinator=true|false     Whether the user is a coordinator for the account
      level=<level>              User admin level
      partition=<part1,part2,...>  Allowed partitions (comma-separated)
      name=<name1,name2,...>    List of user name for batch creation
  
	delete user <name> [account=<account>] [name=<name1,name2,...>]
    Remove a user from the system or from a specific account.
    Parameter details:
      account=<account>          Specify the account (optional)
      name=<name1,name2,...>    Name of users to delete (comma-separated)
  
	show user [accounts=<account>] [name=<name1,name2,...>]
    Display information about users.
    Parameter details:
      accounts=<account>         Show users of this account only
      name=<name1,name2,...>    Show only these users (comma-separated)
      (If not specified, all users will be displayed)
  
	block account <name> [account=<account>]
    Block the specified account from submitting jobs.
    Parameter details:
      name                      Name of the account to block
      account=<account>         Specify the account context (optional)
  
	block user <name> [account=<account>]
    Block the specified user from submitting jobs.
    Parameter details:
      name                      Name of the user to block
      account=<account>         Specify the account context (optional)
  
	unblock account <name> [account=<account>]
    Unblock the specified account to allow job submission.
    Parameter details:
      name                      Name of the account to unblock
      account=<account>         Specify the account context (optional)
  
	unblock user <name> [account=<account>]
    Unblock the specified user to allow job submission.
    Parameter details:
      name                      Name of the user to unblock
      account=<account>         Specify the account context (optional)
  
	add qos <name> [description=<desc>] [priority=<priority>] 
		[maxJobsPerUser=<num>] [maxCpusPerUser=<num>] [maxTimeLimitPerTask=<seconds>]
    Create a new QoS with the specified attributes.
    Parameter details:
      description=<desc>         Description of the QoS
      priority=<priority>        Priority (higher value means higher priority)
      maxJobsPerUser=<num>       Maximum number of jobs per user
      maxCpusPerUser=<num>       Maximum number of CPUs per user
      maxTimeLimitPerTask=<seconds>  Maximum run time per task in seconds
      name=<name1,name2,...>    List of QoS name for batch creation
  
	delete qos <name> [name=<name1,name2,...>]
    Remove the specified QoS from the system.
    Parameter details:
      name=<name1,name2,...>    Name of QoS to delete (comma-separated)
  
	show qos <name> [name=<name1,name2,...>]
    Display information about QoS.
    Parameter details:
      name=<name1,name2,...>    Show only these QoS (comma-separated)
      (If not specified, all QoS will be displayed)

  modify <entity> where [OPTIONS] set [OPTIONS]
    Modify attributes of an existing account, user, or QoS.
    Account options:
      where name=<account>         Specify the account to modify
      set defaultQos=<qos>         Set default QoS
      set allowedPartition+=<part1,part2,...>   Add allowed partitions
      set allowedQos+=<qos1,qos2,...>          Add allowed QoS
      set allowedPartition-=<part1,part2,...> Delete allowed partitions
      set allowedQos-=<qos1,qos2,...>        Delete allowed QoS
      set allowedPartition=<part1,part2,...>      Set allowed partitions directly
      set allowedQos=<qos1,qos2,...>              Set allowed QoS directly
    User options:
      where name=<user> [account=<account>] [partition=<part1,part2,...>]
      set adminlevel=<level>         Set user admin level
      set defaultaccount=<account>   Set default account
      set defaultqos=<qos>           Set default QoS
      set allowedPartition+=...    Add allowed partitions
      set allowedQos+=...          Add allowed QoS
      set allowedPartition-=... Delete allowed partitions
      set allowedQos-=...       Delete allowed QoS
      set allowedPartition=...       Set allowed partitions directly
      set allowedQos=...             Set allowed QoS directly
    QoS options:
      where name=<qos>
      set description=<desc>         Set description
      set maxcpusperuser=<num>        Set max CPUs per user
      set maxsubmitjobsperuser=<num> Set max jobs per user
      set maxtimelimitpertask=<sec>  Set max time per task (seconds)
      set priority=<priority>        Set priority

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
