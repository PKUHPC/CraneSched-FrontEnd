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
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type AuthConfig struct {
	Auth string `json:"auth"`
}

type RegistryConfig struct {
	Auths map[string]AuthConfig `json:"auths"`
}

func encodeAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func decodeAuth(encodedAuth string) (username, password string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(encodedAuth)
	if err != nil {
		return "", "", err
	}

	auth := string(decoded)
	parts := strings.SplitN(auth, ":", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid auth format")
	}

	return parts[0], parts[1], nil
}

func getRegistryConfigPath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDir, util.DefaultUserConfigPrefix, "registry.json"), nil
}

func loadRegistryConfig() (*RegistryConfig, error) {
	configPath, err := getRegistryConfigPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get registry config path: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return &RegistryConfig{Auths: make(map[string]AuthConfig)}, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config RegistryConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}

	if config.Auths == nil {
		config.Auths = make(map[string]AuthConfig)
	}

	return &config, nil
}

func saveRegistryConfig(config *RegistryConfig) error {
	configPath, err := getRegistryConfigPath()
	if err != nil {
		return fmt.Errorf("failed to get registry config path: %v", err)
	}
	configDir := filepath.Dir(configPath)

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %v", err)
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %v", err)
	}

	if err := os.WriteFile(configPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %v", err)
	}

	return nil
}

func readPassword(fromStdin bool) (string, error) {
	if fromStdin {
		reader := bufio.NewReader(os.Stdin)
		password, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(password), nil
	}

	fmt.Print("Password: ")
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println()
	return string(bytePassword), nil
}

func normalizeServerAddress(server string) string {
	if !strings.HasPrefix(server, "http://") && !strings.HasPrefix(server, "https://") {
		server = "http://" + server
	}
	return server
}

func loginExecute(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "registry server is required")
	}

	server := normalizeServerAddress(args[0])

	f := GetFlags()
	username := f.Login.Username
	password := f.Login.Password

	if username == "" {
		fmt.Print("Username: ")
		fmt.Scanln(&username)
		if username == "" {
			return util.NewCraneErr(util.ErrorCmdArg, "username is required")
		}
	}

	if password == "" {
		var err error
		password, err = readPassword(f.Login.PasswordStdin)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "failed to read password: %v", err)
		}
		if password == "" {
			return util.NewCraneErr(util.ErrorCmdArg, "password is required")
		}
	}

	config, err := loadRegistryConfig()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to load registry config: %v", err)
	}

	authConfig := AuthConfig{
		Auth: encodeAuth(username, password),
	}

	config.Auths[server] = authConfig

	if err := saveRegistryConfig(config); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to save registry config: %v", err)
	}

	if f.Global.Json {
		outputJson("login", "", f.Login, config)
	} else {
		fmt.Printf("Login Succeeded: %s\n", server)
	}

	return nil
}

// parseImageRef extracts registry, repository and tag from image reference
// e.g., "registry.example.com/myapp:latest" -> ("registry.example.com", "myapp", "latest")
// e.g., "nginx:latest" -> ("", "nginx", "latest")
// e.g., "nginx" -> ("", "nginx", "latest")
// e.g., "localhost:5000/myapp:latest" -> ("localhost:5000", "myapp", "latest")
// e.g., "myapp@sha256:abcd1234..." -> ("", "myapp", "sha256:abcd1234...")
func parseImageRef(image string) (registry, repository, tag string) {
	// First, check for digest references (image@sha256:...)
	var imagePart string
	if digestIdx := strings.Index(image, "@"); digestIdx != -1 {
		imagePart = image[:digestIdx]
		tag = image[digestIdx+1:] // Include the @ prefix in tag for digest
	} else {
		imagePart = image
	}

	// Split by first slash to separate registry from repository
	parts := strings.SplitN(imagePart, "/", 2)

	var repoWithTag string
	if len(parts) == 2 {
		// Check if first part contains dot, port, or is localhost (likely a registry)
		if strings.Contains(parts[0], ".") || strings.Contains(parts[0], ":") || parts[0] == "localhost" {
			registry = parts[0]
			repoWithTag = parts[1]
		} else {
			// First part is likely a namespace, not a registry
			repoWithTag = imagePart
		}
	} else {
		repoWithTag = imagePart
	}

	// If we already have a digest, don't split on colon
	if tag != "" {
		repository = repoWithTag
		return registry, repository, tag
	}

	// Split repository part by colon to separate repository from tag
	repoParts := strings.SplitN(repoWithTag, ":", 2)
	repository = repoParts[0]

	if len(repoParts) == 2 {
		tag = repoParts[1]
	} else {
		tag = "latest"
	}

	return registry, repository, tag
}

// getAuthForRegistry retrieves saved authentication info for a registry
func getAuthForRegistry(registry string) (username, password string, err error) {
	if registry == "" {
		// Default to docker.io for images without registry
		registry = "docker.io"
	}

	registry = normalizeServerAddress(registry)

	config, err := loadRegistryConfig()
	if err != nil {
		return "", "", err
	}

	authConfig, exists := config.Auths[registry]
	if !exists {
		return "", "", nil // No auth info available
	}

	return decodeAuth(authConfig.Auth)
}

func logoutExecute(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "registry server is required")
	}

	server := normalizeServerAddress(args[0])

	config, err := loadRegistryConfig()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to load registry config: %v", err)
	}

	if _, exists := config.Auths[server]; !exists {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("no existing login for %s", server))
	}

	delete(config.Auths, server)

	if err := saveRegistryConfig(config); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to save registry config: %v", err)
	}

	f := GetFlags()
	if f.Global.Json {
		outputJson("logout", "", nil, config)
	} else {
		fmt.Printf("Logout Succeeded: %s\n", server)
	}

	return nil
}
