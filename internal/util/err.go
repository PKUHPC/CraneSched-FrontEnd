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
