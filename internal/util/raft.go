package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"fmt"
)

var ps *PersistentStorage

func GetLeaderIdFromFile() int {
	ps = NewPersistentStorage(DefaultCraneBaseDir + DefaultPersistentDataPath)
	if ps == nil {
		return -2
	}
	err := ps.LoadData()
	if err != nil {
		return -2
	}
	return ps.data.LeaderId
}

func UpdateLeaderIdToFile(leaderId int) {
	ps.data.LeaderId = leaderId
	err := ps.SaveData()
	if err != nil {
		return
	}
}

func CurrentLeaderId() int {
	return ps.data.LeaderId
}

func QueryLeaderFromCtld(config *Config) int {
	fmt.Println("Attempting to query current leader ID")
	var stub protos.CraneCtldClient
	l := len(config.ControlMachine)
	for i := 1; i < l; i++ {
		id := (ps.data.LeaderId + i) % l
		stub = GetStubToCtldByConfigAndLeaderId(config, id)

		req := &protos.QueryLeaderIdRequest{}

		reply, err := stub.QueryLeaderId(context.Background(), req)
		if err == nil {
			return int(reply.LeaderId)
		}
	}
	return -2
}
