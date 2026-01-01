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

	"github.com/distribution/reference"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type ImageRef struct {
	Image         string // fully normalized for CRI
	ServerAddress string // registry host[:port]
}

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

func loginExecute(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "registry server is required")
	}

	server := strings.TrimSpace(args[0])
	server = strings.TrimPrefix(server, "http://")
	server = strings.TrimPrefix(server, "https://")
	server = strings.TrimSuffix(server, "/")
	if server == "" || strings.Contains(server, "/") {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid registry server: %s", args[0]))
	}

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

func NormalizeImageRef(input string) (ImageRef, error) {
	in := strings.TrimSpace(input)
	if in == "" {
		return ImageRef{}, fmt.Errorf("image reference is empty")
	}

	// Parse + normalize "familiar" Docker image names.
	named, err := reference.ParseDockerRef(in)
	if err != nil {
		return ImageRef{}, err
	}

	return ImageRef{
		Image:         named.String(),
		ServerAddress: reference.Domain(named),
	}, nil
}

// getAuthForRegistry retrieves saved authentication info for a registry
func getAuthForRegistry(registry string) (username, password string, err error) {
	if registry == "" {
		return "", "", fmt.Errorf("registry cannot be empty")
	}

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

	server := strings.TrimSpace(args[0])
	server = strings.TrimPrefix(server, "http://")
	server = strings.TrimPrefix(server, "https://")
	server = strings.TrimSuffix(server, "/")
	if server == "" || strings.Contains(server, "/") {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid registry server: %s", args[0]))
	}

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
