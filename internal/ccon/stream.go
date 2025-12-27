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

package ccon

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/term"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

type StreamOptions struct {
	Stdin     bool
	Stdout    bool
	Stderr    bool
	Tty       bool
	Transport string // "spdy" or "ws"
}

type TerminalSizeQueue struct {
	resizeCh chan remotecommand.TerminalSize
	done     chan struct{}
}

func NewTerminalSizeQueue() *TerminalSizeQueue {
	return &TerminalSizeQueue{
		resizeCh: make(chan remotecommand.TerminalSize, 1),
		done:     make(chan struct{}),
	}
}

func (t *TerminalSizeQueue) Next() *remotecommand.TerminalSize {
	select {
	case size := <-t.resizeCh:
		return &size
	case <-t.done:
		return nil
	}
}

func (t *TerminalSizeQueue) Stop() {
	close(t.done)
}

func (t *TerminalSizeQueue) monitorResize() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGWINCH)

	go func() {
		defer signal.Stop(sigCh)
		for {
			select {
			case <-sigCh:
				size := t.getTerminalSize()
				if size != nil {
					select {
					case t.resizeCh <- *size:
					default:
					}
				}
			case <-t.done:
				return
			}
		}
	}()

	initialSize := t.getTerminalSize()
	if initialSize != nil {
		select {
		case t.resizeCh <- *initialSize:
		default:
		}
	}
}

func (t *TerminalSizeQueue) getTerminalSize() *remotecommand.TerminalSize {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return nil
	}

	width, height, err := term.GetSize(fd)
	if err != nil {
		log.Debugf("Failed to get terminal size: %v", err)
		return nil
	}

	return &remotecommand.TerminalSize{
		Width:  uint16(width),
		Height: uint16(height),
	}
}

func StreamWithURL(ctx context.Context, streamURL string, opts StreamOptions) error {
	return streamWithRetry(ctx, streamURL, opts, 3)
}

func streamWithRetry(ctx context.Context, streamURL string, opts StreamOptions, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Debugf("Stream attempt %d/%d for URL: %s", attempt, maxRetries, streamURL)

		err := doStream(ctx, streamURL, opts)
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}

		lastErr = err
		log.Warnf("Stream attempt %d failed: %v", attempt, err)

		if attempt < maxRetries {
			// Wait a bit before retrying, especially for server readiness issues
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
	}

	return fmt.Errorf("stream failed after %d attempts, last error: %w", maxRetries, lastErr)
}

func doStream(ctx context.Context, streamURL string, opts StreamOptions) error {
	parsedURL, err := url.Parse(streamURL)
	if err != nil {
		return fmt.Errorf("invalid stream URL: %w", err)
	}

	config := &rest.Config{
		Host: fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Host),
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}

	var stdin io.Reader
	var stdout, stderr io.Writer
	var tty bool

	if opts.Stdin {
		stdin = os.Stdin
	}
	if opts.Stdout {
		stdout = os.Stdout
	}
	if opts.Stderr && !opts.Tty {
		stderr = os.Stderr
	}
	tty = opts.Tty

	if tty && term.IsTerminal(int(os.Stdin.Fd())) {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			return fmt.Errorf("failed to set terminal to raw mode: %w", err)
		}
		defer func() {
			if err := term.Restore(int(os.Stdin.Fd()), oldState); err != nil {
				log.Errorf("Failed to restore terminal: %v", err)
			}
		}()
	}

	streamOpts := remotecommand.StreamOptions{
		Stdin:             stdin,
		Stdout:            stdout,
		Stderr:            stderr,
		Tty:               tty,
		TerminalSizeQueue: nil,
	}

	if tty {
		sizeQueue := NewTerminalSizeQueue()
		defer sizeQueue.Stop()
		sizeQueue.monitorResize()
		streamOpts.TerminalSizeQueue = sizeQueue
	}

	executor, err := createExecutor(config, parsedURL, opts.Transport)
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	log.Debugf("Starting stream with URL: %s (stdin=%t, stdout=%t, stderr=%t, tty=%t)",
		streamURL, opts.Stdin, opts.Stdout, opts.Stderr, opts.Tty)

	return executor.StreamWithContext(ctx, streamOpts)
}

func createExecutor(config *rest.Config, parsedURL *url.URL, transport string) (remotecommand.Executor, error) {
	tr, err := rest.TransportFor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	if httpTransport, ok := tr.(*http.Transport); ok {
		if httpTransport.TLSClientConfig == nil {
			httpTransport.TLSClientConfig = &tls.Config{}
		}
		httpTransport.TLSClientConfig.InsecureSkipVerify = true
	}

	switch transport {
	case "spdy":
		return createSPDYExecutor(config, parsedURL)
	case "ws":
		return createWebSocketExecutor(config, parsedURL)
	default:
		return nil, fmt.Errorf("unsupported transport: %s (supported: spdy, ws)", transport)
	}
}

func createSPDYExecutor(config *rest.Config, parsedURL *url.URL) (remotecommand.Executor, error) {
	spdyExecutor, err := remotecommand.NewSPDYExecutor(config, "POST", parsedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create SPDY executor: %w", err)
	}
	log.Debug("Using SPDY executor")
	return spdyExecutor, nil
}

func createWebSocketExecutor(config *rest.Config, parsedURL *url.URL) (remotecommand.Executor, error) {
	wsExecutor, err := remotecommand.NewWebSocketExecutor(config, "GET", parsedURL.String())
	if err != nil {
		return nil, fmt.Errorf("failed to create WebSocket executor: %w", err)
	}
	log.Debug("Using WebSocket executor")
	return wsExecutor, nil
}
