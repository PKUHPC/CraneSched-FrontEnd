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

package cattach

import (
	"CraneFrontEnd/generated/protos"
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
)

// X11GlobalId uniquely identifies an X11 session by the craned node and the
// local connection ID assigned by the supervisor.
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

// X11Session represents a single X11 forwarding connection between a remote
// X11 client (on the compute node) and the local X11 server.
type X11Session struct {
	X11Id X11GlobalId
	// Data from the remote X11 client; nil signals EOF / close.
	X11ToLocal      chan []byte
	X11ToSupervisor chan *protos.StreamCattachRequest
	Status          X11Status
	sessionMgr      *X11SessionMgr
	stopReadWrite   *sync.Once
	eofSent         *sync.Once
	conn            net.Conn
}

// X11SessionMgr manages all concurrent X11 sessions for a single cattach
// connection.  It mirrors the X11SessionMgr in internal/crun/x11.go but uses
// the StreamCattachRequest / StreamCattachReply proto types.
type X11SessionMgr struct {
	sessionMutex sync.Mutex
	x11Sessions  map[X11GlobalId]*X11Session
	// All X11 reply messages from cfored (STEP_X11_CONN, STEP_X11_FORWARD, STEP_X11_EOF).
	X11ReplyChan chan *protos.StreamCattachReply
	// Outgoing X11 data from local sessions to cfored / supervisor.
	X11RequestChan chan *protos.StreamCattachRequest
	finishCtx      *context.Context

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

// SendEofToSupervisor sends a final (possibly empty) data packet to the
// supervisor to signal that the local X11 connection has been closed.
func (session *X11Session) SendEofToSupervisor(data []byte) {
	session.eofSent.Do(func() {
		log.Debugf("[X11 %s:%d] Sending EOF to supervisor.", session.X11Id.CranedId, session.X11Id.LocalId)
		req := &protos.StreamCattachRequest{
			Type: protos.StreamCattachRequest_STEP_X11_FORWARD,
			Payload: &protos.StreamCattachRequest_PayloadStepX11ForwardReq{
				PayloadStepX11ForwardReq: &protos.StreamCattachRequest_StepX11ForwardReq{
					Msg:      data,
					CranedId: session.X11Id.CranedId,
					LocalId:  session.X11Id.LocalId,
				},
			},
		}
		session.X11ToSupervisor <- req
	})
}

// StopLocalReadWrite signals the writer goroutine (via a nil sentinel) to
// close the local X11 connection and stop reading/writing.
func (session *X11Session) StopLocalReadWrite() {
	session.stopReadWrite.Do(func() {
		session.X11ToLocal <- nil
	})
}

// StatusConnectingLocal dials the local X11 display and transitions the
// session to X11Forwarding (or X11Ended on failure).
func (session *X11Session) StatusConnectingLocal() {
	var err error
	if session.sessionMgr.port == 0 { // Unix socket
		session.conn, err = net.Dial("unix", session.sessionMgr.target)
		if err != nil {
			log.Errorf("[X11 %s:%d] Failed to connect to X11 display by unix: %v",
				session.X11Id.CranedId, session.X11Id.LocalId, err)
			session.Status = X11Ended
			session.SendEofToSupervisor(make([]byte, 0))
			return
		}
	} else { // TCP socket
		address := net.JoinHostPort(session.sessionMgr.target, fmt.Sprintf("%d", session.sessionMgr.port))
		session.conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Errorf("[X11 %s:%d] Failed to connect to X11 display by tcp: %v",
				session.X11Id.CranedId, session.X11Id.LocalId, err)
			session.Status = X11Ended
			session.SendEofToSupervisor(make([]byte, 0))
			return
		}
	}
	log.Debugf("[X11 %s:%d] X11 session connected to local X11 server.",
		session.X11Id.CranedId, session.X11Id.LocalId)
	session.Status = X11Forwarding
}

// StatusForwarding runs the bidirectional data relay between the local X11
// connection and the supervisor (via cfored).
func (session *X11Session) StatusForwarding() {
	wg := sync.WaitGroup{}
	wg.Add(2)

	// Reader: local X11 server → supervisor
	go func() {
		reader := bufio.NewReader(session.conn)
		buffer := make([]byte, 4096)
		for {
			n, err := reader.Read(buffer)
			if err != nil {
				if err == io.EOF {
					log.Tracef("[X11 %s:%d] X11 fd reached EOF, stop reading.",
						session.X11Id.CranedId, session.X11Id.LocalId)
					data := make([]byte, n)
					copy(data, buffer[:n])
					session.SendEofToSupervisor(data)
					break
				}
				log.Tracef("[X11 %s:%d] X11 fd closed, stop reading: %v",
					session.X11Id.CranedId, session.X11Id.LocalId, err)
				break
			}
			data := make([]byte, n)
			copy(data, buffer[:n])
			req := &protos.StreamCattachRequest{
				Type: protos.StreamCattachRequest_STEP_X11_FORWARD,
				Payload: &protos.StreamCattachRequest_PayloadStepX11ForwardReq{
					PayloadStepX11ForwardReq: &protos.StreamCattachRequest_StepX11ForwardReq{
						Msg:      data,
						CranedId: session.X11Id.CranedId,
						LocalId:  session.X11Id.LocalId,
					},
				},
			}
			log.Tracef("[X11 %s:%d] Received data from x11 fd (len %d)",
				session.X11Id.CranedId, session.X11Id.LocalId, len(data))
			select {
			case session.X11ToSupervisor <- req:
				log.Tracef("[X11 %s:%d] Sent data to supervisor.",
					session.X11Id.CranedId, session.X11Id.LocalId)
			default:
				log.Errorf("[X11 %s:%d] X11 to supervisor channel full, dropping data.",
					session.X11Id.CranedId, session.X11Id.LocalId)
			}
		}
		log.Tracef("[X11 %s:%d] X11 session reader ended.",
			session.X11Id.CranedId, session.X11Id.LocalId)
		wg.Done()
	}()

	// Writer: supervisor → local X11 server
	go func() {
	loop:
		for {
			select {
			case msg := <-session.X11ToLocal:
				if msg == nil {
					log.Tracef("[X11 %s:%d] X11 session received EOF to local, stop writing.",
						session.X11Id.CranedId, session.X11Id.LocalId)
					err := session.conn.Close()
					log.Debugf("[X11 %s:%d] X11 session closed local connection.",
						session.X11Id.CranedId, session.X11Id.LocalId)
					if err != nil {
						log.Errorf("[X11 %s:%d] Error closing x11 connection: %v",
							session.X11Id.CranedId, session.X11Id.LocalId, err)
					}
					break loop
				}
				log.Tracef("[X11 %s:%d] Writing to x11 fd len[%d].",
					session.X11Id.CranedId, session.X11Id.LocalId, len(msg))
				_, err := session.conn.Write(msg)
				if err != nil {
					log.Errorf("[X11 %s:%d] Failed to write to x11 fd: %v, stop writing.",
						session.X11Id.CranedId, session.X11Id.LocalId, err)
					break loop
				}
			}
		}
		log.Tracef("[X11 %s:%d] X11 session writer ended.",
			session.X11Id.CranedId, session.X11Id.LocalId)
		session.SendEofToSupervisor(make([]byte, 0))
		wg.Done()
	}()

	wg.Wait()
	session.Status = X11Ended
}

// NewX11SessionMgr creates a new X11 session manager for a cattach connection.
func NewX11SessionMgr(meta *protos.X11Meta, finishCtx *context.Context) *X11SessionMgr {
	return &X11SessionMgr{
		sessionMutex:   sync.Mutex{},
		x11Sessions:    make(map[X11GlobalId]*X11Session),
		X11ReplyChan:   make(chan *protos.StreamCattachReply, 64),
		X11RequestChan: make(chan *protos.StreamCattachRequest, 64),
		finishCtx:      finishCtx,
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

// SessionMgrRoutine is the main event loop for the X11 session manager.
// It routes incoming X11 messages from cfored to the correct session and
// tears down all sessions when the task finishes.
func (sm *X11SessionMgr) SessionMgrRoutine() {
	for {
		select {
		case reply := <-sm.X11ReplyChan:
			switch reply.Type {
			case protos.StreamCattachReply_STEP_X11_CONN:
				payload := reply.GetPayloadStepX11ConnReply()
				cranedId := payload.GetCranedId()
				localId := payload.GetLocalId()
				log.Tracef("[Cattach X11] New X11 connection from craned %s local id %d", cranedId, localId)
				globalId := X11GlobalId{
					CranedId: cranedId,
					LocalId:  localId,
				}
				session := sm.NewSession(globalId)
				sm.sessionMutex.Lock()
				sm.x11Sessions[globalId] = session
				go session.SessionRoutine()
				sm.sessionMutex.Unlock()

			case protos.StreamCattachReply_STEP_X11_FORWARD:
				payload := reply.GetPayloadStepX11ForwardReply()
				cranedId := payload.GetCranedId()
				localId := payload.GetLocalId()
				data := payload.GetMsg()
				log.Tracef("[Cattach X11 %s:%d] Forward data len %d", cranedId, localId, len(data))
				globalId := X11GlobalId{
					CranedId: cranedId,
					LocalId:  localId,
				}
				sm.sessionMutex.Lock()
				session, exists := sm.x11Sessions[globalId]
				if exists {
					session.X11ToLocal <- data
				} else {
					log.Warnf("[Cattach X11 %s:%d] Received STEP_X11_FORWARD for non-existing session",
						cranedId, localId)
				}
				sm.sessionMutex.Unlock()

			case protos.StreamCattachReply_STEP_X11_EOF:
				payload := reply.GetPayloadStepX11EofReply()
				cranedId := payload.GetCranedId()
				localId := payload.GetLocalId()
				log.Tracef("[Cattach X11 %s:%d] X11 EOF received", cranedId, localId)
				globalId := X11GlobalId{
					CranedId: cranedId,
					LocalId:  localId,
				}
				sm.sessionMutex.Lock()
				session, exists := sm.x11Sessions[globalId]
				if exists {
					session.X11ToLocal <- nil
					log.Tracef("[Cattach X11 %s:%d] Signalled session EOF", cranedId, localId)
				} else {
					log.Warnf("[Cattach X11 %s:%d] Received STEP_X11_EOF for non-existing session",
						cranedId, localId)
				}
				sm.sessionMutex.Unlock()
			}

		case <-(*sm.finishCtx).Done():
			log.Tracef("[Cattach X11] Received finish signal, terminating all X11 sessions")
			sm.sessionMutex.Lock()
			for id, session := range sm.x11Sessions {
				log.Tracef("[Cattach X11 %s:%d] Stopping X11 session", id.CranedId, id.LocalId)
				session.StopLocalReadWrite()
			}
			sm.sessionMutex.Unlock()
			return
		}
	}
}
