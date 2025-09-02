package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
)

var (
	stub protos.CraneCtldClient
)

func FillReqByCobraFlags() (*protos.QueryTasksInfoRequest, error) {
	req:= protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: false}

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
			return nil , err
		}
	}
	
	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}

	return &req,nil
}

func QueryTasksInfo() (*protos.QueryTasksInfoReply, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)

	req, err := FillReqByCobraFlags()
	if (err != nil) {
		return  &protos.QueryTasksInfoReply{}, err
	}
	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job queue")
		return reply, &util.CraneError{Code: util.ErrorNetwork}
	}

	return reply, nil
}