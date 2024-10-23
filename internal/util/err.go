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

func ErrMsg(err_code protos.ErrCode) string {
	switch err_code {
	case protos.ErrCode_ERR_INVALID_UID:
		return "The user UID being operated on does not exist in the system"
	case protos.ErrCode_ERR_INVALID_OP_USER:
		return "you are not a user of Crane"
	case protos.ErrCode_ERR_INVALID_USER:
		return "The entered user is not a user of Crane"
	case protos.ErrCode_ERR_PERMISSION_USER:
		return "Your permission is insufficient"
	case protos.ErrCode_ERR_USER_DUPLICATE_ACCOUNT:
		return "The user already exists in this account"
	case protos.ErrCode_ERR_USER_ALLOWED_ACCOUNT:
		return "The user is not allowed to access account"
	case protos.ErrCode_ERR_INVALID_ADMIN_LEVEL:
		return "Unknown admin level"
	case protos.ErrCode_ERR_USER_ACCOUNT_MISMATCH:
		return "The user does not belong to this account"
	case protos.ErrCode_ERR_NO_ACCOUNT_SPECIFIED:
		return "No account is specified for the user"
	default:
		break
	}

	switch err_code {
	case protos.ErrCode_ERR_INVALID_ACCOUNT:
		return "The entered account does not exist"
	case protos.ErrCode_ERR_INVALID_PARENTACCOUNT:
		return "The parent account of the entered account does not exist"
	case protos.ErrCode_ERR_DUPLICATE_ACCOUNT:
		return "The account already exists in the crane"
	case protos.ErrCode_ERR_DELETE_ACCOUNT:
		return "The account has child account or users, unable to delete."
	}

	switch err_code {
	case protos.ErrCode_ERR_INVALID_PARTITION:
		return "The entered partition does not exist"
	case protos.ErrCode_ERR_ALLOWED_PARTITION:
		return "The entered account or user does not include this partition"
	case protos.ErrCode_ERR_PARENT_ALLOWED_PARTITION:
		return "Parent account does not include the partition"
	case protos.ErrCode_ERR_DUPLICATE_PARTITION:
		return "The partition already exists in the account or user"
	case protos.ErrCode_ERR_USER_EMPTY_PARTITION:
		return "The user does not contain any partitions, operation cannot be performed."
	case protos.ErrCode_ERR_CHILD_HAS_PARTITION:
		return "The partition is currently being used by the child accounts or users of the account, operation cannot be performed. You can use a forced operation to ignore this constraint"
	}

	switch err_code {
	case protos.ErrCode_ERR_INVALID_QOS:
		return "The entered qos does not exist"
	case protos.ErrCode_ERR_DB_DUPLICATE_QOS:
		return "Qos already exists in the crane"
	case protos.ErrCode_ERR_DELETE_QOS:
		return "QoS is still being used by accounts or users, unable to delete"
	case protos.ErrCode_ERR_CONVERT_TO_INTERGER:
		return "Failed to convert value to integer"
	case protos.ErrCode_ERR_TIME_LIMIT:
		return "Invalid time limit value"
	case protos.ErrCode_ERR_ALLOWED_QOS:
		return "The entered account or user does not include this qos"
	case protos.ErrCode_ERR_DUPLICATE_QOS:
		return "The Qos already exists in the account or user"
	case protos.ErrCode_ERR_PARENT_ALLOWED_QOS:
		return "Parent account does not include the qos"
	case protos.ErrCode_ERR_SET_ALLOWED_QOS:
		return "The entered QoS list does not include the default QoS for this user. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list"
	case protos.ErrCode_ERR_ALLOWED_DEFAULT_QOS:
		return "The entered default_qos is not allowed"
	case protos.ErrCode_ERR_DUPLICATE_DEFAULT_QOS:
		return "The QoS is already the default QoS for the account or specified partition of the user"
	case protos.ErrCode_ERR_SET_ACCOUNT_QOS:
		return "The entered QoS list does not include the default QoS for this account or some descendant node. You can use a forced operation to ignore this constraint"
	case protos.ErrCode_ERR_CHILD_HAS_DEFAULT_QOS:
		return "some child accounts or users is using the QoS as the default QoS. By ignoring this constraint with forced deletion, the deleted default QoS is randomly replaced with one of the remaining items in the QoS list"
	case protos.ErrCode_ERR_SET_DEFAULT_QOS:
		return "The Qos not allowed or is already the default qos"
	case protos.ErrCode_ERR_IS_DEFAULT_QOS:
		return "The QoS is the default QoS for the current user/Account and cannot be modified. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list"
	}

	switch err_code {
	case protos.ErrCode_ERR_UPDATE_DATABASE:
		return "Fail to update data in database"
	default:
		break
	}

	return fmt.Sprintf("Unknown Error Occurred: %s", err_code)
}
