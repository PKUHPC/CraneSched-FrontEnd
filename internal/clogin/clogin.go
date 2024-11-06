package clogin

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"

	log "github.com/sirupsen/logrus"
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
	// TODO: 保存token
	err = util.SaveFileWithPermissions(util.DefaultJwtTokenPath, []byte(reply.GetToken()), 0600)
	if err != nil {
		log.Errorf(err.Error())
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}
