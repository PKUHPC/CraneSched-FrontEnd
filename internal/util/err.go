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

package util

import (
	"CraneFrontEnd/generated/protos"
	"fmt"
)

type CraneCmdError = int

// Do not use error code bigger than 127

// general
const (
	ErrorSuccess       CraneCmdError = 0
	ErrorGeneric       CraneCmdError = 1
	ErrorCmdArg        CraneCmdError = 2
	ErrorNetwork       CraneCmdError = 3
	ErrorBackend       CraneCmdError = 4
	ErrorInvalidFormat CraneCmdError = 5
)

var errMsgMap = map[protos.ErrCode]string{

	// User-related errors
	protos.ErrCode_ERR_INVALID_UID:                   "The user UID being operated on does not exist in the system",
	protos.ErrCode_ERR_INVALID_OP_USER:               "You are not a user of Crane",
	protos.ErrCode_ERR_INVALID_USER:                  "The entered user is not a user of Crane",
	protos.ErrCode_ERR_PERMISSION_USER:               "Your permission is insufficient",
	protos.ErrCode_ERR_BLOCKED_USER:                  "The user has been blocked",
	protos.ErrCode_ERR_USER_ALREADY_EXISTS:           "The user already exists in this account",
	protos.ErrCode_ERR_USER_ACCESS_TO_ACCOUNT_DENIED: "The user is not allowed to access account",
	protos.ErrCode_ERR_INVALID_ADMIN_LEVEL:           "Unknown admin level",
	protos.ErrCode_ERR_USER_ACCOUNT_MISMATCH:         "The user does not belong to this account",
	protos.ErrCode_ERR_NO_ACCOUNT_SPECIFIED:          "No account is specified for the user",

	// Account-related errors
	protos.ErrCode_ERR_INVALID_ACCOUNT:        "The entered account does not exist",
	protos.ErrCode_ERR_INVALID_PARENT_ACCOUNT: "The parent account of the entered account does not exist",
	protos.ErrCode_ERR_ACCOUNT_ALREADY_EXISTS: "The account already exists in the crane",
	protos.ErrCode_ERR_ACCOUNT_HAS_CHILDREN:   "The account has child account or users, unable to delete.",
	protos.ErrCode_ERR_BLOCKED_ACCOUNT:        "The account has been blocked",

	// Partition-related errors
	protos.ErrCode_ERR_INVALID_PARTITION:                "The entered partition does not exist",
	protos.ErrCode_ERR_PARTITION_MISSING:                "The entered account or user does not include this partition",
	protos.ErrCode_ERR_PARENT_ACCOUNT_PARTITION_MISSING: "Parent account does not include the partition",
	protos.ErrCode_ERR_PARTITION_ALREADY_EXISTS:         "The partition already exists in the account or user",
	protos.ErrCode_ERR_USER_EMPTY_PARTITION:             "The user does not contain any partitions, operation cannot be performed.",
	protos.ErrCode_ERR_CHILD_HAS_PARTITION:              "The partition is currently being used by the child accounts or users of the account, operation cannot be performed. You can use a forced operation to ignore this constraint",
	protos.ErrCode_ERR_HAS_NO_QOS_IN_PARTITION:          "The user has no QoS available for this partition to be used",
	protos.ErrCode_ERR_HAS_ALLOWED_QOS_IN_PARTITION:     "The qos you set is not in partition's allowed qos list",

	// QoS-related errors
	protos.ErrCode_ERR_INVALID_QOS:                     "The entered qos does not exist",
	protos.ErrCode_ERR_DB_QOS_ALREADY_EXISTS:           "Qos already exists in the crane",
	protos.ErrCode_ERR_QOS_REFERENCES_EXIST:            "QoS is still being used by accounts or users, unable to delete",
	protos.ErrCode_ERR_CONVERT_TO_INTEGER:              "Failed to convert value to integer",
	protos.ErrCode_ERR_TIME_LIMIT:                      "Invalid time limit value",
	protos.ErrCode_ERR_QOS_MISSING:                     "The entered account or user does not include this qos",
	protos.ErrCode_ERR_QOS_ALREADY_EXISTS:              "The Qos already exists in the account or user",
	protos.ErrCode_ERR_PARENT_ACCOUNT_QOS_MISSING:      "Parent account does not include the qos",
	protos.ErrCode_ERR_SET_ALLOWED_QOS:                 "The entered QoS list does not include the default QoS for this user. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list",
	protos.ErrCode_ERR_DEFAULT_QOS_NOT_INHERITED:       "The entered default_qos is not allowed",
	protos.ErrCode_ERR_DUPLICATE_DEFAULT_QOS:           "The QoS is already the default QoS for the account or specified partition of the user",
	protos.ErrCode_ERR_SET_ACCOUNT_QOS:                 "The entered QoS list does not include the default QoS for this account or some descendant node. You can use a forced operation to ignore this constraint",
	protos.ErrCode_ERR_CHILD_HAS_DEFAULT_QOS:           "some child accounts or users is using the QoS as the default QoS. By ignoring this constraint with forced deletion, the deleted default QoS is randomly replaced with one of the remaining items in the QoS list",
	protos.ErrCode_ERR_SET_DEFAULT_QOS:                 "The Qos not allowed or is already the default qos",
	protos.ErrCode_ERR_DEFAULT_QOS_MODIFICATION_DENIED: "The QoS is the default QoS for the current user/Account and cannot be modified. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list",

	// System-related errors
	protos.ErrCode_ERR_UPDATE_DATABASE:        "Fail to update data in database",
	protos.ErrCode_ERR_NO_RESOURCE:            "Resource not enough for task",
	protos.ErrCode_ERR_INVALID_NODE_NUM:       "Nodes partition not enough for task",
	protos.ErrCode_ERR_INVAILD_NODE_LIST:      "Invalid node list",
	protos.ErrCode_ERR_INVAILD_EX_NODE_LIST:   "Invalid exclude node list",
	protos.ErrCode_ERR_TIME_TIMIT_BEYOND:      "Time-limit reached the user's limit",
	protos.ErrCode_ERR_CPUS_PER_TASK_BEYOND:   "cpus-per-task reached the user's limit",
	protos.ErrCode_ERR_NO_ENOUGH_NODE:         "Nodes num not enough for task",
	protos.ErrCode_ERR_BEYOND_TASK_ID:         "System error occurred or the number of pending tasks exceeded maximum value",
	protos.ErrCode_ERR_CGROUP:                 "Error when manipulating cgroup",
	protos.ErrCode_ERR_SYSTEM_ERR:             "Linux Error",
	protos.ErrCode_ERR_RPC_FAILURE:            "RPC call failed",
	protos.ErrCode_ERR_GENERIC_FAILURE:        "Generic failure",
	protos.ErrCode_ERR_NON_EXISTENT:           "The object doesn't exist",
	protos.ErrCode_ERR_INVALID_PARAM:          "Invalid Parameter",
	protos.ErrCode_ERR_PROTOBUF:               "Error when using protobuf",
	protos.ErrCode_ERR_MAX_JOB_COUNT_PER_USER: "job max count is empty or exceeds the limit",
	protos.ErrCode_ERR_USER_NO_PRIVILEGE:      "User has insufficient privilege",
}

func ErrMsg(err_code protos.ErrCode) string {
	if msg, exists := errMsgMap[err_code]; exists {
		return msg
	}
	return fmt.Sprintf("Unknown Error Occurred: %s", err_code)
}
