/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
)

type CranedChannelKeeper struct {
	crunRequestChannelMtx sync.Mutex
	crunRequestChannelCV  *sync.Cond

	// Request message from crun to craned
	crunRequestChannelMapByCranedId map[string]chan *protos.StreamCrunRequest
}

var gCranedChanKeeper *CranedChannelKeeper

func NewCranedChannelKeeper() *CranedChannelKeeper {
	keeper := &CranedChannelKeeper{}
	keeper.crunRequestChannelCV = sync.NewCond(&keeper.crunRequestChannelMtx)
	keeper.crunRequestChannelMapByCranedId = make(map[string]chan *protos.StreamCrunRequest)
	return keeper
}

func (keeper *CranedChannelKeeper) cranedChannelIsUp(cranedId string, msgChannel chan *protos.StreamCrunRequest) {
	keeper.crunRequestChannelMtx.Lock()
	keeper.crunRequestChannelMapByCranedId[cranedId] = msgChannel
	keeper.crunRequestChannelCV.Broadcast()
	keeper.crunRequestChannelMtx.Unlock()
}

func (keeper *CranedChannelKeeper) craneChannelIsDown(cranedId string) {
	keeper.crunRequestChannelMtx.Lock()
	delete(keeper.crunRequestChannelMapByCranedId, cranedId)
	keeper.crunRequestChannelMtx.Unlock()
}

func (keeper *CranedChannelKeeper) waitCranedChannelsReady(cranedIds []string, readyChan chan bool, stopWaiting *atomic.Bool) {
	keeper.crunRequestChannelMtx.Lock()
	defer keeper.crunRequestChannelMtx.Unlock()
	for !stopWaiting.Load() {
		allReady := true
		for _, node := range cranedIds {
			_, exits := keeper.crunRequestChannelMapByCranedId[node]
			if !exits {
				allReady = false
				break
			}
		}

		if !allReady {
			keeper.crunRequestChannelCV.Wait() // gVars.crunRequestChannelMtx is unlocked.
			// Once Wait() returns, the lock is held again.
		} else {
			log.Debug("[Cfored<->Crun] All related craned up now")
			readyChan <- true
			break
		}
	}
}

func (keeper *CranedChannelKeeper) sendRequestToCranedChannel(request *protos.StreamCrunRequest, cranedIds []string) {
	keeper.crunRequestChannelMtx.Lock()
	for _, node := range cranedIds {
		keeper.crunRequestChannelMapByCranedId[node] <- request
	}
	keeper.crunRequestChannelMtx.Unlock()
}

type GrpcCforedServer struct {
	protos.CraneForeDServer
}

type grpcMessage[T any] struct {
	message *T
	err     error
}

type grpcStreamServer[RecvT any] interface {
	Recv() (*RecvT, error)
	grpc.ServerStream
}

func grpcStreamReceiver[RecvT any](stream grpcStreamServer[RecvT], requestChannel chan grpcMessage[RecvT]) {
	for {
		callocRequest, err := stream.Recv()
		requestChannel <- grpcMessage[RecvT]{
			message: callocRequest,
			err:     err,
		}
		if err != nil {
			break
		}
	}
}

func (cforedServer *GrpcCforedServer) QueryTaskIdFromPort(ctx context.Context,
	request *protos.QueryTaskIdFromPortRequest) (*protos.QueryTaskIdFromPortReply, error) {

	var taskId uint32
	var ok bool

	pid, err := util.GetPidFromPort(uint16(request.Port))
	if err != nil {
		return &protos.QueryTaskIdFromPortReply{Ok: false}, nil
	}

	for {
		gVars.pidTaskIdMapMtx.RLock()
		taskId, ok = gVars.pidTaskIdMap[int32(pid)]
		gVars.pidTaskIdMapMtx.RUnlock()

		if ok {
			return &protos.QueryTaskIdFromPortReply{
				Ok:     true,
				TaskId: taskId,
			}, nil
		}

		pid, err = util.GetParentProcessID(pid)
		if err != nil || pid == 1 {
			return &protos.QueryTaskIdFromPortReply{Ok: false}, nil
		}
	}
}
