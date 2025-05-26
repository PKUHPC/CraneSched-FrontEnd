package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
)

var ps *PersistentStorage

func GetLeaderIdFromFile() int {
	ps = NewPersistentStorage(DefaultTempBaseDir + DefaultPersistentDataPath)
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

func HostName2ServerId(config *Config, hostname string) int {
	for i := 0; i < len(config.ControlMachine); i++ {
		if config.ControlMachine[i].Hostname == hostname {
			return i
		}
	}

	return -2
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
			if reply.LeaderId >= 0 {
				return int(reply.LeaderId)
			} else {
				fmt.Printf("server #%d return to an abnormal state, leader ID: %d\n", id, reply.LeaderId)
			}
		}
	}
	return -2
}

func QueryAndUpdateLeaderId(config *Config) {
	id := QueryLeaderFromCtld(config)
	if id >= 0 {
		UpdateLeaderIdToFile(id)
		log.Printf("Leader ID was changed to %d, please try again!\n", id)
	} else {
		log.Errorln("Failed to query current leader ID, broken backend.")
	}
}
