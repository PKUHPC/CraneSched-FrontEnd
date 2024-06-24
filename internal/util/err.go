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

type CraneErrorType = int

// Do not use error code bigger than 127

// general
const (
	ErrorSuccess       CraneErrorType = 0
	ErrorGeneric       CraneErrorType = 1
	ErrorCmdArg        CraneErrorType = 2
	ErrorNetwork       CraneErrorType = 3
	ErrorBackend       CraneErrorType = 4
	ErrorInvalidFormat CraneErrorType = 5
)

type CraneError struct {
	Code    CraneErrorType
	Message string
}

func (e *CraneError) Error() string {
	return e.Message
}
