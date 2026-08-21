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

package crun

import (
	"errors"
	"sync"

	"CraneFrontEnd/generated/protos"
)

var errCrunStreamSenderClosed = errors.New("crun stream sender is closed")

type crunRequestStream interface {
	Send(*protos.StreamCrunRequest) error
}

type crunStreamSendItem struct {
	request  *protos.StreamCrunRequest
	terminal bool
	result   chan error
}

type crunStreamSender struct {
	mu               sync.Mutex
	requestQueue     []crunStreamSendItem
	requestAvailable chan struct{}
	done             chan struct{}
	closeOnce        sync.Once
	closed           bool
	terminalAdmitted bool
}

func newCrunStreamSender(stream crunRequestStream) *crunStreamSender {
	sender := &crunStreamSender{
		requestAvailable: make(chan struct{}, 1),
		done:             make(chan struct{}),
	}
	go sender.run(stream)
	return sender
}

func (s *crunStreamSender) Send(request *protos.StreamCrunRequest) error {
	return s.send(request, false)
}

func (s *crunStreamSender) SendTerminal(request *protos.StreamCrunRequest) error {
	return s.send(request, true)
}

func (s *crunStreamSender) send(request *protos.StreamCrunRequest, terminal bool) error {
	item := crunStreamSendItem{
		request:  request,
		terminal: terminal,
		result:   make(chan error, 1),
	}

	s.mu.Lock()
	if s.closed || s.terminalAdmitted {
		s.mu.Unlock()
		return errCrunStreamSenderClosed
	}
	if terminal {
		s.terminalAdmitted = true
	}
	s.requestQueue = append(s.requestQueue, item)
	s.mu.Unlock()

	s.notifyWorker()
	return <-item.result
}

func (s *crunStreamSender) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		pending := s.requestQueue
		s.requestQueue = nil
		s.mu.Unlock()

		for _, item := range pending {
			item.result <- errCrunStreamSenderClosed
		}
		close(s.done)
		s.notifyWorker()
	})
}

func (s *crunStreamSender) run(stream crunRequestStream) {
	for {
		item, ok := s.nextRequest()
		if !ok {
			return
		}

		err := stream.Send(item.request)
		item.result <- err
		if item.terminal || err != nil {
			s.Close()
			return
		}
	}
}

func (s *crunStreamSender) nextRequest() (crunStreamSendItem, bool) {
	for {
		s.mu.Lock()
		if len(s.requestQueue) > 0 {
			item := s.requestQueue[0]
			s.requestQueue[0] = crunStreamSendItem{}
			s.requestQueue = s.requestQueue[1:]
			s.mu.Unlock()
			return item, true
		}
		closed := s.closed
		s.mu.Unlock()

		if closed {
			return crunStreamSendItem{}, false
		}

		select {
		case <-s.requestAvailable:
		case <-s.done:
			return crunStreamSendItem{}, false
		}
	}
}

func (s *crunStreamSender) notifyWorker() {
	select {
	case s.requestAvailable <- struct{}{}:
	default:
	}
}
