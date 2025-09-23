package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"time"
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
    // Use a 10-second timeout for the RPC  
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)  
    defer cancel()  

    reply, err := stub.QueryTasksInfo(ctx, req)  
    if err != nil {  
        util.GrpcErrorPrintf(err, "Failed to query task queue")  
        return nil, &util.CraneError{Code: util.ErrorNetwork}  
    } 

	return reply, nil
}