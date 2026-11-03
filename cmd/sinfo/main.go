// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"CraneFrontEnd/compat/sinfo"
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	var signalCode atomic.Int32
	go func() {
		select {
		case sig := <-signals:
			signalCode.Store(128 + int32(sig.(syscall.Signal)))
			cancel()
		case <-ctx.Done():
		}
	}()
	code := sinfo.Run(ctx, os.Args, os.Environ(), os.Stdout, os.Stderr)
	signal.Stop(signals)
	cancel()
	if received := signalCode.Load(); received != 0 {
		code = int(received)
	}
	os.Exit(code)
}
