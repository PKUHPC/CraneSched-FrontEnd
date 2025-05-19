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
  
  USAGE: cacctmgr <ACTION> <RESOURCE> [OPTIONS]
  
  ACTIONS:
	add       - Create a new account, user, or QoS
	delete    - Remove an account, user, or QoS
	block     - Block an account or user
	unblock   - Unblock an account or user
	modify    - Change attributes of an account, user, or QoS
	show      - Display basic information
	find      - Search for specific resources
  
  RESOURCES:
	account   - User account in the system
	user      - Individual user
	qos       - Quality of Service settings
  
  COMMANDS:
	add account <name> [description=<desc>] [parent=<parent>] [default-qos=<qos>] 
				[partition=<part1,part2,...>] [qos-list=<qos1,qos2,...>]
	  Create a new account with the specified attributes.
  
	delete account <name>
	  Remove an account from the system.
  
	block account <name> [account=<account>]
	  Block an account, preventing job submissions.
  
	unblock account <name> [account=<account>]
	  Unblock a previously blocked account.
  
	show account
	  Display information about all accounts.
  
	find account <name>
	  Show detailed information about a specific account.
  
	add user <name> account=<account> [coordinator=true|false] [level=<level>] 
			[partition=<part1,part2,...>]
	  Create a new user associated with an account.
  
	delete user <name> [account=<account>]
	  Remove a user from the system or from a specific account.
  
	block user name=<name> [account=<account>]
	  Block a user, preventing job submissions.
  
	unblock user name=<name> [account=<account>]
	  Unblock a previously blocked user.
  
	show user [accounts=<account>]
	  Display information about users, optionally filtered by account.
  
	find user <name> [account=<account>]
	  Show detailed information about a specific user.
  
	add qos <name> [description=<desc>] [priority=<priority>] 
			[maxJobsPerUser=<num>] [maxCpusPerUser=<num>] [maxTimeLimitPerTask=<seconds>]
	  Create a new QoS with the specified attributes.
  
	delete qos <name>
	  Remove a QoS from the system.
  
	find qos <name>
	  Show detailed information about a specific QoS.

  	modify <resource> where [OPTIONS] set [OPTIONS]
	  Modify attributes of an existing user.
	  OPTIONS:
        modify account  (set options) addAllowedPartition=, 
						addAllowedQos=, deleteAllowedPartition=,
						deleteAllowedQos=, allowedPartition=,
						allowedQos=, defaultQos=
                        (where options) name=

		modify qos      (set options) description=,
                        maxcpuperuser=, maxsubmitjobsperuser=,
                        maxtimelimitpertask=, priority=
                        (where options) name=

		modify user     (set options) adminlevel=, defaultaccount=,
                        comment=, defaultqos=, allowedpartition=,
                        allowedQos=, deleteAllowedPartition=,
                        deleteAllowedQos=, setAllowedPartition=,
                        setAllowedQos=
                        (where options) accounts=, name=, partitions=
  GLOBAL OPTIONS:
	--help, -h     Display this help message
	--config, -C     Specify an alternative configuration file (default: /etc/crane/config.yaml)
	--json, -J     Format output as JSON
	--version, -v    Display the version of the program

  NOTE: Parameters in [] are optional. Parameters in <> should be replaced with actual values.
  `
	fmt.Println(help)
}
