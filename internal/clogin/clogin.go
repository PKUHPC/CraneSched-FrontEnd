package clogin

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
)

var (
	userUid uint32
	stub    protos.CraneCtldClient
)

func Login(password string) util.CraneCmdError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	req := protos.LoginRequest{Uid: userUid, Password: password}
	var reply *protos.LoginReply
	var err error

	reply, err = stub.Login(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to login")
		return util.ErrorNetwork
	}

	if !reply.GetOk() {
		fmt.Printf("Login failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}

	err = util.SaveFileWithPermissions(util.DefaultJwtTokenPath, []byte(reply.GetToken()), 0600)
	if err != nil {
		fmt.Printf("Failed to save token file: %s. \n", err.Error())
		return util.ErrorGeneric
	}
	fmt.Println("Login succeeded.")
	return util.ErrorSuccess
}
