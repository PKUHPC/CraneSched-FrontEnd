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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

func logExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("log requires exactly one argument: CONTAINER")
	}

	container := args[0]

	// Mock log entries
	mockLogs := []string{
		"2025-01-15T10:30:00.123Z Starting container...",
		"2025-01-15T10:30:01.456Z Application initialized",
		"2025-01-15T10:30:02.789Z Server listening on port 8080",
		"2025-01-15T10:30:05.012Z Received HTTP request: GET /health",
		"2025-01-15T10:30:05.345Z Health check passed",
		"2025-01-15T10:30:10.678Z Received HTTP request: GET /api/users",
		"2025-01-15T10:30:10.901Z Database connection established",
		"2025-01-15T10:30:11.234Z Query executed successfully",
		"2025-01-15T10:30:11.567Z Response sent: 200 OK",
		"2025-01-15T10:30:15.890Z Background task completed",
	}

	var logsToShow []string
	if FlagTail > 0 && FlagTail < len(mockLogs) {
		logsToShow = mockLogs[len(mockLogs)-FlagTail:]
	} else {
		logsToShow = mockLogs
	}

	if FlagJson {
		result := map[string]interface{}{
			"action":     "log",
			"container":  container,
			"logs":       logsToShow,
			"follow":     FlagFollow,
			"tail":       FlagTail,
			"timestamps": FlagTimestamps,
		}
		jsonData, _ := json.Marshal(result)
		fmt.Println(string(jsonData))
	} else {
		for _, log := range logsToShow {
			if FlagTimestamps {
				fmt.Println(log)
			} else {
				// Remove timestamp prefix for cleaner output
				if len(log) > 24 && log[23] == ' ' {
					fmt.Println(log[24:])
				} else {
					fmt.Println(log)
				}
			}
		}
		if FlagFollow {
			fmt.Printf("[Following logs for container %s... Press Ctrl+C to stop]\n", container)
		}
	}

	return nil
}