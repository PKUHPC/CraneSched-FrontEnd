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
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type Container struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Image   string            `json:"image"`
	Status  string            `json:"status"`
	Created time.Time         `json:"created"`
	Ports   []string          `json:"ports"`
	Env     map[string]string `json:"env"`
	Volumes []string          `json:"volumes"`
}

func psExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return errors.New("ps command does not accept any arguments")
	}

	mockContainers := []Container{
		{
			ID:      "abc123def456",
			Name:    "web-server",
			Image:   "nginx:latest",
			Status:  "running",
			Created: time.Now().Add(-2 * time.Hour),
			Ports:   []string{"80:8080", "443:8443"},
		},
		{
			ID:      "def456ghi789",
			Name:    "database",
			Image:   "postgres:13",
			Status:  "running",
			Created: time.Now().Add(-24 * time.Hour),
			Ports:   []string{"5432:5432"},
		},
		{
			ID:      "ghi789jkl012",
			Name:    "worker",
			Image:   "redis:alpine",
			Status:  "stopped",
			Created: time.Now().Add(-72 * time.Hour),
			Ports:   []string{},
		},
	}

	var containers []Container
	if FlagAll {
		containers = mockContainers
	} else {
		for _, c := range mockContainers {
			if c.Status == "running" {
				containers = append(containers, c)
			}
		}
	}

	if FlagJson {
		jsonData, _ := json.Marshal(containers)
		fmt.Println(string(jsonData))
	} else {
		if FlagQuiet {
			for _, c := range containers {
				fmt.Println(c.ID[:12])
			}
		} else {
			fmt.Printf("%-12s %-15s %-20s %-10s %-15s %-20s\n",
				"CONTAINER ID", "IMAGE", "NAME", "STATUS", "PORTS", "CREATED")
			for _, c := range containers {
				portsStr := strings.Join(c.Ports, ",")
				if portsStr == "" {
					portsStr = "-"
				}
				createdStr := formatDuration(time.Since(c.Created)) + " ago"
				fmt.Printf("%-12s %-15s %-20s %-10s %-15s %-20s\n",
					c.ID[:12], c.Image, c.Name, c.Status, portsStr, createdStr)
			}
		}
	}

	return nil
}

func inspectExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("inspect requires exactly one argument: CONTAINER")
	}

	container := args[0]

	mockContainer := Container{
		ID:      "abc123def456789",
		Name:    container,
		Image:   "nginx:latest",
		Status:  "running",
		Created: time.Now().Add(-2 * time.Hour),
		Ports:   []string{"80:8080", "443:8443"},
		Env: map[string]string{
			"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			"HOME": "/root",
		},
		Volumes: []string{"/app:/usr/share/nginx/html", "/config:/etc/nginx/conf.d"},
	}

	if FlagJson {
		jsonData, _ := json.Marshal(mockContainer)
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Container: %s\n", mockContainer.Name)
		fmt.Printf("  ID: %s\n", mockContainer.ID)
		fmt.Printf("  Image: %s\n", mockContainer.Image)
		fmt.Printf("  Status: %s\n", mockContainer.Status)
		fmt.Printf("  Created: %s\n", mockContainer.Created.Format(time.RFC3339))
		fmt.Printf("  Ports:\n")
		for _, port := range mockContainer.Ports {
			fmt.Printf("    %s\n", port)
		}
		fmt.Printf("  Environment:\n")
		for key, value := range mockContainer.Env {
			fmt.Printf("    %s=%s\n", key, value)
		}
		fmt.Printf("  Volumes:\n")
		for _, volume := range mockContainer.Volumes {
			fmt.Printf("    %s\n", volume)
		}
	}

	return nil
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d seconds", int(d.Seconds()))
	} else if d < time.Hour {
		return fmt.Sprintf("%d minutes", int(d.Minutes()))
	} else if d < 24*time.Hour {
		return fmt.Sprintf("%d hours", int(d.Hours()))
	} else {
		return fmt.Sprintf("%d days", int(d.Hours()/24))
	}
}