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
		return "op user is not a user of Crane"
	case protos.ErrCode_ERR_INVALID_USER:
		return "The entered user is not a user of Crane"
	case protos.ErrCode_ERR_PERMISSION_USER:
		return "Your permission is insufficient"
	case protos.ErrCode_ERR_USER_DUPLICATE_ACCOUNT:
		return "The user already exists in this account"
	case protos.ErrCode_ERR_USER_ALLOWED_ACCOUNT:
		return ""
	case protos.ErrCode_ERR_USER_ALLOWED_USER:
		return ""
	case protos.ErrCode_ERR_INVALID_ADMIN_LEVEL:
		return ""
	case protos.ErrCode_ERR_USER_ACCOUNT_MISMATCH:
		return ""
	default:
		break
	}

	switch err_code {
	case protos.ErrCode_ERR_INVALID_ACCOUNT:
		return "The entered account does not exist"
	}

	switch err_code {
	case protos.ErrCode_ERR_INVALID_PARTITION:
		return "The entered partition does not exist"
	case protos.ErrCode_ERR_ALLOWED_PARTITION:
		return "The account or user does not include this partition"
	case protos.ErrCode_ERR_PARENT_ALLOWED_PARTITION:
		return "Parent account does not include the partition"
	case protos.ErrCode_ERR_DUPLICATE_PARTITION:
		return "The partition is already in allowed partition list"
	}

	switch err_code {
	case protos.ErrCode_ERR_UPDATE_DATABASE:
		return "Fail to update data in database"
	default:
		break
	}

	return fmt.Sprintf("Unknown Error Occurred: %s", err_code)
}
