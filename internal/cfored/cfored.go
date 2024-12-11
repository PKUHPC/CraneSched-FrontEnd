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

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"os"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type GlobalVariables struct {
	hostName string

	ctldConnected atomic.Bool

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	// Prevent the situation that de-multiplexer the two maps when
	// Cfored <--> Ctld state machine just removes the channel of a calloc
	// from ctldReplyChannelMapByPid and hasn't added the channel
	// to ctldReplyChannelMapByTaskId. In such a situation, Cfored <--> Ctld
	// state machine may think the channel of a calloc is not in both maps
	// but the channel is actually being moving from one map to another map.
	ctldReplyChannelMapMtx sync.Mutex

	// Used by Cfored <--> Ctld state machine to de-multiplex messages
	// Used for calloc/crun with task id not allocated.
	// A calloc is identified by its pid.
	ctldReplyChannelMapByPid map[int32]chan *protos.StreamCtldReply

	// Used by Cfored <--> Ctld state machine to de-multiplex messages from CraneCtld.
	// Cfored <--> Ctld state machine GUARANTEES that NO `nil` will be sent into these channels.
	// Used for calloc/crun with task id allocated.
	ctldReplyChannelMapByTaskId map[uint32]chan *protos.StreamCtldReply

	// Used by Calloc/Crun <--> Cfored state machine to multiplex messages
	// these messages will be sent to CraneCtld
	cforedRequestCtldChannel chan *protos.StreamCforedRequest

	pidTaskIdMapMtx sync.RWMutex

	pidTaskIdMap map[int32]uint32
}

var gVars GlobalVariables

func StartCfored() {
	util.InitLogger(FlagDebugLevel)

	config := util.ParseConfig(FlagConfigFilePath)

	util.DetectNetworkProxy()

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())
	defer gVars.globalCtxCancel()

	gVars.ctldConnected.Store(false)

	gVars.cforedRequestCtldChannel = make(chan *protos.StreamCforedRequest, 8)

	gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapByTaskId = make(map[uint32]chan *protos.StreamCtldReply)
	gVars.pidTaskIdMap = make(map[int32]uint32)

	gCranedChanKeeper = NewCranedChannelKeeper()

	hostName, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %s", err.Error())
	}
	gVars.hostName = hostName

	var wgAllRoutines sync.WaitGroup

	ctldClient := &GrpcCtldClient{
		ctldClientStub:   util.GetStubToCtldByConfig(config),
		ctldReplyChannel: make(chan *protos.StreamCtldReply, 8),
	}
	wgAllRoutines.Add(1)
	go ctldClient.StartCtldClientStream(&wgAllRoutines)

	startGrpcServer(config, &wgAllRoutines)

	log.Debug("Waiting all go routines to exit...")
	wgAllRoutines.Wait()
}
