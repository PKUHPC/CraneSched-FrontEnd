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
	"sync/atomic"

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
	requestChannel chan crunStreamSendItem
	done           chan struct{}
	closeOnce      sync.Once
	accepting      atomic.Bool
}

func newCrunStreamSender(stream crunRequestStream) *crunStreamSender {
	sender := &crunStreamSender{
		requestChannel: make(chan crunStreamSendItem),
		done:           make(chan struct{}),
	}
	sender.accepting.Store(true)
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
	if !s.accepting.Load() {
		return errCrunStreamSenderClosed
	}

	item := crunStreamSendItem{
		request:  request,
		terminal: terminal,
		result:   make(chan error, 1),
	}

	select {
	case s.requestChannel <- item:
		return <-item.result
	case <-s.done:
		return errCrunStreamSenderClosed
	}
}

func (s *crunStreamSender) Close() {
	s.closeOnce.Do(func() {
		s.accepting.Store(false)
		close(s.done)
	})
}

func (s *crunStreamSender) run(stream crunRequestStream) {
	for {
		select {
		case item := <-s.requestChannel:
			err := stream.Send(item.request)
			item.result <- err
			if item.terminal || err != nil {
				s.Close()
				return
			}
		case <-s.done:
			return
		}
	}
}
