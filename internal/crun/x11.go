/**
 * Copyright (c) 2025 Peking University and Peking University
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

package crun

import (
	"CraneFrontEnd/generated/protos"
	"bufio"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
)

type X11GlobalId struct {
	CranedId string
	LocalId  uint32
}

type X11Status int

const (
	X11ConnectingLocal X11Status = 0
	X11Forwarding      X11Status = 1
	X11Ended           X11Status = 2
)

type X11Session struct {
	X11Id X11GlobalId
	// Data from task, nil if eof, will close local connection, stop read/write
	X11ToLocal      chan []byte
	X11ToSupervisor chan *protos.StreamCrunRequest
	Status          X11Status
	sessionMgr      *X11SessionMgr
	stopReadWrite   *sync.Once
	eofSent         *sync.Once
	conn            net.Conn
}
type X11SessionMgr struct {
	sessionMutex sync.Mutex
	x11Sessions  map[X11GlobalId]*X11Session
	//All x11 reply from cfored
	X11ReplyChan chan *protos.StreamCrunReply
	//Any msg on chan close all x11 fwd session
	X11RequestChan chan *protos.StreamCrunRequest
	finishCtx      *context.Context
	errCtx         *context.Context

	port   uint32
	target string
}

func (session *X11Session) SessionRoutine() {
X11StatusMachine:
	for {
		switch session.Status {
		case X11ConnectingLocal:
			session.StatusConnectingLocal()
		case X11Forwarding:
			log.Tracef("[X11 %s:%d] X11 session forwarding.", session.X11Id.CranedId, session.X11Id.LocalId)
			session.StatusForwarding()
		case X11Ended:
			session.sessionMgr.sessionMutex.Lock()
			delete(session.sessionMgr.x11Sessions, session.X11Id)
			session.sessionMgr.sessionMutex.Unlock()
			log.Tracef("[X11 %s:%d] X11 session ended and removed.", session.X11Id.CranedId, session.X11Id.LocalId)

			break X11StatusMachine
		}
	}
}

func (session *X11Session) SendEofToSupervisor(data []byte) {
	session.eofSent.Do(func() {
		req := &protos.StreamCrunRequest{
			Type: protos.StreamCrunRequest_TASK_X11_FORWARD,
			Payload: &protos.StreamCrunRequest_PayloadTaskX11ForwardReq{
				PayloadTaskX11ForwardReq: &protos.StreamCrunRequest_TaskX11ForwardReq{
					Msg:      data,
					CranedId: session.X11Id.CranedId,
					LocalId:  session.X11Id.LocalId,
				},
			},
		}
		session.X11ToSupervisor <- req
	})
}

func (session *X11Session) StopLocalReadWrite() {
	session.stopReadWrite.Do(func() {
		session.X11ToLocal <- nil
	})
}

func (session *X11Session) StatusConnectingLocal() {
	var err error
	if session.sessionMgr.port == 0 { // Unix Socket
		session.conn, err = net.Dial("unix", session.sessionMgr.target)
		if err != nil {
			log.Errorf("[X11 %s:%d] Failed to connect to X11 display by unix: %v", session.X11Id.CranedId, session.X11Id.LocalId, err)
			session.Status = X11Ended
			session.SendEofToSupervisor(make([]byte, 0))
			return
		}
	} else { // TCP socket
		address := net.JoinHostPort(session.sessionMgr.target, fmt.Sprintf("%d", session.sessionMgr.port))
		session.conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Errorf("[X11 %s:%d] Failed to connect to X11 display by tcp: %v", session.X11Id.CranedId, session.X11Id.LocalId, err)
			session.Status = X11Ended
			session.SendEofToSupervisor(make([]byte, 0))
			return
		}
	}
	log.Debugf("[X11 %s:%d] X11 session connected local X11 server.", session.X11Id.CranedId, session.X11Id.LocalId)
	session.Status = X11Forwarding
	return
}

func (session *X11Session) StatusForwarding() {
	//Reader
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		reader := bufio.NewReader(session.conn)
		buffer := make([]byte, 4096)
		for {
			n, err := reader.Read(buffer)
			if err != nil {
				if err == io.EOF {
					log.Tracef("[X11 %s:%d] X11 fd reached EOF, stop reading.", session.X11Id.CranedId, session.X11Id.LocalId)
					data := make([]byte, n)
					copy(data, buffer[:n])
					session.SendEofToSupervisor(data)
					break
				}
				log.Tracef("[X11 %s:%d] X11 fd has been closed and stop reading: %v", session.X11Id.CranedId, session.X11Id.LocalId, err)
				break
			}
			data := make([]byte, n)
			copy(data, buffer[:n])
			req := &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_X11_FORWARD,
				Payload: &protos.StreamCrunRequest_PayloadTaskX11ForwardReq{
					PayloadTaskX11ForwardReq: &protos.StreamCrunRequest_TaskX11ForwardReq{
						Msg:      data,
						CranedId: session.X11Id.CranedId,
						LocalId:  session.X11Id.LocalId,
					},
				},
			}
			log.Tracef("[X11 %s:%d] Received data from x11 fd (len %d)", session.X11Id.CranedId, session.X11Id.LocalId, len(data))
			session.X11ToSupervisor <- req
		}

		wg.Done()
	}()

	go func() {
	loop:
		for {
			select {
			case msg := <-session.X11ToLocal:
				if msg == nil {
					log.Tracef("[X11 %s:%d] X11 session received eof to local, stop writing.", session.X11Id.CranedId, session.X11Id.LocalId)
					err := session.conn.Close()
					log.Debugf("[X11 %s:%d] X11 session closed local connection.", session.X11Id.CranedId, session.X11Id.LocalId)
					if err != nil {
						log.Errorf("[X11 %s:%d] Error closing x11 connection: %v", session.X11Id.CranedId, session.X11Id.LocalId, err)
					}
					break loop
				}
				log.Tracef("[X11 %s:%d] Writing to x11 fd.", session.X11Id.CranedId, session.X11Id.LocalId)
				_, err := session.conn.Write(msg)
				if err != nil {
					log.Errorf("[X11 %s:%d] Failed to write to x11 fd: %v, stop writing.", session.X11Id.CranedId, session.X11Id.LocalId, err)
					break loop
				}
			}
		}
		session.SendEofToSupervisor(make([]byte, 0))
		wg.Done()
	}()

	wg.Wait()
	session.Status = X11Ended
}

func NewX11SessionMgr(meta *protos.X11Meta, taskFinishCtx *context.Context, taskErrCtx *context.Context) *X11SessionMgr {
	return &X11SessionMgr{
		sessionMutex:   sync.Mutex{},
		x11Sessions:    make(map[X11GlobalId]*X11Session),
		X11ReplyChan:   make(chan *protos.StreamCrunReply, 64),
		X11RequestChan: make(chan *protos.StreamCrunRequest, 64),
		finishCtx:      taskFinishCtx,
		errCtx:         taskErrCtx,
		port:           meta.Port,
		target:         meta.Target,
	}
}

func (sm *X11SessionMgr) NewSession(id X11GlobalId) *X11Session {
	return &X11Session{
		X11Id:           id,
		X11ToLocal:      make(chan []byte, 64),
		X11ToSupervisor: sm.X11RequestChan,
		Status:          X11ConnectingLocal,
		sessionMgr:      sm,
		conn:            nil,
		eofSent:         &sync.Once{},
		stopReadWrite:   &sync.Once{},
	}
}

func (sm *X11SessionMgr) SessionMgrRoutine() {

	for {
		select {
		case reply := <-sm.X11ReplyChan:
			switch reply.Type {
			case protos.StreamCrunReply_TASK_X11_CONN:
				payload := reply.GetPayloadTaskX11ConnReply()
				cranedId := payload.GetCranedId()
				localId := payload.GetLocalId()
				log.Tracef("X11 connection request from craned %s local id %d", cranedId, localId)
				globalId := X11GlobalId{
					CranedId: cranedId,
					LocalId:  localId,
				}
				session := sm.NewSession(globalId)
				sm.sessionMutex.Lock()
				go session.SessionRoutine()
				sm.x11Sessions[globalId] = session
				sm.sessionMutex.Unlock()
			case protos.StreamCrunReply_TASK_X11_FORWARD:
				payload := reply.GetPayloadTaskX11ForwardReply()
				cranedId := payload.GetCranedId()
				localId := payload.GetLocalId()
				data := payload.GetMsg()
				log.Tracef("[X11 %s:%d] forward data len %d", cranedId, localId, len(data))
				globalId := X11GlobalId{
					CranedId: cranedId,
					LocalId:  localId,
				}
				sm.sessionMutex.Lock()
				session, exists := sm.x11Sessions[globalId]

				sm.sessionMutex.Unlock()
				if exists {
					session.X11ToLocal <- data
				} else {
					log.Warnf("[X11 %s:%d] Received X11 forward for non-existing session ", cranedId, localId)
				}
			case protos.StreamCrunReply_TASK_X11_EOF:
				payload := reply.GetPayloadTaskX11EofReply()
				cranedId := payload.GetCranedId()
				localId := payload.GetLocalId()
				log.Tracef("[X11 %s:%d] X11 EOF", cranedId, localId)
				globalId := X11GlobalId{
					CranedId: cranedId,
					LocalId:  localId,
				}
				sm.sessionMutex.Lock()
				session, exists := sm.x11Sessions[globalId]
				sm.sessionMutex.Unlock()
				if exists {
					session.X11ToLocal <- nil
					log.Tracef("[X11 %s:%d] Removed X11 session ", cranedId, localId)
				} else {
					log.Warnf("[X11 %s:%d] Received X11 EOF for non-existing session", cranedId, localId)
				}
			}
		case <-(*sm.finishCtx).Done():
			log.Tracef("Received X11 finish signal, terminating all X11 sessions")
			sm.sessionMutex.Lock()
			for id, session := range sm.x11Sessions {
				log.Tracef("[X11 %s:%d] Stopping X11 session", id.CranedId, id.LocalId)
				session.StopLocalReadWrite()
			}
			sm.sessionMutex.Unlock()
			return
		case <-(*sm.errCtx).Done():
			log.Tracef("Received X11 finish signal, terminating all X11 sessions")
			sm.sessionMutex.Lock()
			for id, session := range sm.x11Sessions {
				log.Tracef("Stopping X11 session craned %s local id %d", id.CranedId, id.LocalId)
				session.StopLocalReadWrite()
			}
			sm.sessionMutex.Unlock()
			return
		}
	}
}
