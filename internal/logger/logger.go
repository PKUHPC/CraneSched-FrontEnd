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

package logger

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
)

var (
	stub protos.CraneCtldClient
)

func SetLogLevel(logger, logLevel string) util.CraneCmdError {
	if err := util.CheckLogLevel(logLevel); err != nil {
		log.Errorf(" Error %v. Valid log levels are: trace, debug, info, warn, error.\n", err)
		return util.ErrorCmdArg
	}
	req := &protos.SetLogLevelRequest{
		Logger:   logger,
		LogLevel: logLevel,
	}
	if stub == nil {
		log.Fatal("gRPC client stub is not initialized")
	}
	reply, err := stub.SetLogerLevel(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to set log_level")
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	fmt.Print(reply.GetReason())
	if reply.GetOk() {
		return util.ErrorSuccess
	} else {
		return util.ErrorBackend
	}
}
