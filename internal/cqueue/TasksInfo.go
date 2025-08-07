package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os/user"
)

var (
	stub protos.CraneCtldClient
)

func ParseFlag(name,value,msg string) (interface{}, error) {
	handler, ok := flagRegistry.Load(name)
	if !ok {
		return nil, fmt.Errorf("flag '%s' not registered", name)
	}
	result, err := handler.(FlagHandler).Parse(value)
	if err != nil {
		return nil, handler.(FlagHandler).Error(msg,err)
	}
	return result, nil
}

func DealStringFlag(req *protos.QueryTasksInfoRequest,name,FlagString ,msg string) (*protos.QueryTasksInfoRequest,error) {
	if FlagString != "" {
    	users, err := ParseFlag(name,FlagString, msg)
    	if err != nil {
        	return nil, err
    	}
    	req.FilterUsers = users.([]string)
	}
	return req, nil
}

func FillReq() (*protos.QueryTasksInfoRequest, error) {
	req := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: false}
	if FlagSelf {
		cu, err := user.Current()
		if err != nil {
			return nil, util.GetCraneError(util.ErrorCmdArg,fmt.Sprintf("Failed to get current username: %s.", err))
		}
		req.FilterUsers = []string{cu.Username}
	}

	if FlagFilterStates != "" {
		stateList, err := ParseFlag("FlagFilterStates",FlagFilterStates,"err")
		if err != nil {
			return nil, err
		}
		req.FilterTaskStates = stateList.([]protos.TaskStatus)
	}
	if  _,err := DealStringFlag(&req,"FlagFilterJobNames",FlagFilterJobNames,"Invalid job name list specified: "); err != nil {
    	return nil, err
	}	
	if  _,err := DealStringFlag(&req,"FlagFilterUsers",FlagFilterUsers,"Invalid User list specified: "); err != nil {
    	return nil, err
	}
	if  _,err := DealStringFlag(&req,"FlagFilterQos",FlagFilterQos,"Invalid Qos list specified: "); err != nil {
    	return nil, err
	}
	if  _,err := DealStringFlag(&req,"FlagFilterAccounts",FlagFilterAccounts,"Invalid account list specified: "); err != nil {
    	return nil, err
	}
	if  _,err := DealStringFlag(&req,"FlagFilterPartitions",FlagFilterPartitions,"Invalid partition list specified: "); err != nil {
    	return nil, err
	}
	if FlagFilterJobIDs != "" {
		filterJobIdList, err := ParseFlag("FlagFilterJobIDs",FlagFilterJobIDs,"Invalid job list specified:")
		if  err != nil {
			return nil, err
		}
		req.FilterTaskIds = filterJobIdList.([]uint32)
		req.NumLimit = uint32(len(filterJobIdList.([]uint32)))
	}

	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}
	return &req, nil
}

func QueryTasksInfo() (*protos.QueryTasksInfoReply, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)

	req, err := FillReq()
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