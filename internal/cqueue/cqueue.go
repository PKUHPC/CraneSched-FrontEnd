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

package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"strconv"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

var (
	stub protos.CraneCtldClient
)

func FillReqByCobraFlags() (*protos.QueryTasksInfoRequest, error) {
	req := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: false}

	processors := []FilterProcessor{
		&StatesFilterProcessor{},
		&SelfProcessor{},
		&JobNamesProcessor{},
		&UserFilterProcessor{},
		&QosProcessor{},
		&AccountProcessor{},
		&PartitionsProcessor{},
		&JobIDsProcessor{},
	}

	for _, p := range processors {
		if err := p.Process(&req); err != nil {
			return nil, err
		}
	}

	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}

	return &req, nil
}

func QueryTasksInfo() (*protos.QueryTasksInfoReply, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)

	req, err := FillReqByCobraFlags()
	if err != nil {
		return &protos.QueryTasksInfoReply{}, err
	}

	// set 10 seconds limit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reply, err := stub.QueryTasksInfo(ctx, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			switch rpcErr.Code() {
			case grpccodes.DeadlineExceeded:
				util.GrpcErrorPrintf(err, "Query time out, due to too many jobs")
				return nil, util.NewCraneErr(util.ErrorBackend,
					"Query timed out due to large number of jobs. Please try with a smaller scope or use filters.",
				)
			case grpccodes.ResourceExhausted:
				util.GrpcErrorPrintf(err, "Response too large")
				return nil, util.NewCraneErr(util.ErrorNetwork,
					"Response too large for gRPC. Please reduce the query scope or avoid using -m with huge values.")
			default:
				util.GrpcErrorPrintf(err, "Failed to query task queue")
				return nil, &util.CraneError{Code: util.ErrorNetwork}
			}
		} else {
			util.GrpcErrorPrintf(err, "Failed to query task queue, grpcstatus get error failed.")
			return nil, &util.CraneError{Code: util.ErrorNetwork}
		}
	}
	return reply, nil
}

func Query() error {
	reply, err := QueryTasksInfo()
	if err != nil {
		return err
	}
	if FlagJson {
		return JsonOutput(reply)
	}
	return QueryTableOutput(reply)
}

func loopedQuery(iterate uint64) error {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, "Invalid time interval.")
	}
	return loopedSubQuery(interval)
}

func loopedSubQuery(interval time.Duration) error {
	for {
		fmt.Println(time.Now().String()[0:19])
		if err := Query(); err != nil {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
