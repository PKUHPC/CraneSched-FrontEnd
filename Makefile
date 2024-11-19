#
# Copyright (c) 2024 Peking University and Peking University
# Changsha Institute for Computing and Digital Economy
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

# Makefile for Golang components of CraneSched.
# This file will generate protobuf files, build executables and plugins. 

# Notes for developer: 
# - Please use proper indentation for neater output.

# Variables
GIT_COMMIT_HASH := $(shell git rev-parse --short HEAD)
VERSION_FILE := VERSION
VERSION := $(shell [ -f $(VERSION_FILE) ] && cat $(VERSION_FILE) || echo $(GIT_COMMIT_HASH))
BUILD_TIME := $(shell date +'%a, %d %b %Y %H:%M:%S %z')
LDFLAGS := -ldflags \
			"-X 'CraneFrontEnd/internal/util.VERSION=$(VERSION)' \
			-X 'CraneFrontEnd/internal/util.BUILD_TIME=$(BUILD_TIME)'"
BIN_DIR := build/bin
PLUGIN_DIR := build/plugin

GO_VERSION := $(shell go version)
GO_PATH := $(shell go env GOROOT)
GO := $(GO_PATH)/bin/go
COMMON_ENV := GOROOT=$(GO_PATH)
BUILD_FLAGS := -trimpath

# Targets
.PHONY: all protos build clean install plugin energy-plugin other-plugins

all: protos build plugin

protos: 
	@echo "- Generating Protobuf files..."
	@mkdir -p ./generated/protos
	@protoc --go_out=generated --go-grpc_out=generated --proto_path=protos protos/*.proto
	@echo "  - Summary:"
	@echo "    - Protobuf files generated in ./generated/protos/"

build:
	@echo "- Building executables with $(GO_VERSION)..."
	@mkdir -p $(BIN_DIR)
	@for dir in cmd/*/ ; do \
		echo "  - Building $$dir"; \
		(cd $$dir && $(COMMON_ENV) $(GO) build $(BUILD_FLAGS) $(LDFLAGS) \
			-o ../../$(BIN_DIR)/$$(basename $$dir)) || exit 1; \
	done
	@echo "  - Summary:"
	@echo "    - Version: $(VERSION)"
	@echo "    - Build time: $(BUILD_TIME)"
	@echo "    - Commit hash: $(GIT_COMMIT_HASH)"
	@echo "    - Binaries are in ./$(BIN_DIR)/"

plugin: energy-plugin other-plugins

# NVIDIA 配置
NVIDIA_LIB_PATH := /usr/lib/x86_64-linux-gnu
CUDA_INCLUDE_PATH := /usr/include

# 设置编译标志
CGO_CFLAGS := -I$(CUDA_INCLUDE_PATH)
CGO_LDFLAGS := -L$(NVIDIA_LIB_PATH) -l:libnvidia-ml.so.565.57.01 -Wl,-rpath,$(NVIDIA_LIB_PATH)

energy-plugin:
	@echo "Building energy plugin with $(GO_VERSION)..."
	@mkdir -p $(PLUGIN_DIR)
	@cd plugin/energy && \
		$(COMMON_ENV) \
		CGO_CFLAGS="$(CGO_CFLAGS)" \
		CGO_LDFLAGS="$(CGO_LDFLAGS)" \
		$(GO) build $(BUILD_FLAGS) -buildmode=plugin \
		-o ../../$(PLUGIN_DIR)/energy.so .


other-plugins:
	@echo "  - Building other plugins..."
	@mkdir -p $(PLUGIN_DIR)
	@for dir in plugin/*/ ; do \
		if [ "$$(basename $$dir)" != "energy" ]; then \
			echo "    - Building: $$(basename $$dir).so"; \
			(cd $$dir && $(COMMON_ENV) $(GO) build $(BUILD_FLAGS) $(LDFLAGS) \
				-buildmode=plugin -o ../../$(PLUGIN_DIR)/$$(basename $$dir).so) || exit 1; \
		fi \
	done
	@echo "  - Summary:"
	@echo "    - Plugins are in ./$(PLUGIN_DIR)/"

clean:
	@echo "Cleaning up..."
	@rm -rf build

install: 
	@echo "- Installing executables, plugins and auxiliary files..."
	@if [ ! -d "$(BIN_DIR)" ]; then \
		echo "Error: $(BIN_DIR) does not exist. Please build first."; \
		exit 1; \
	fi
	@echo "  - Installing binaries to /usr/local/bin/"
	@mkdir -p /usr/local/bin
	@cp $(BIN_DIR)/* /usr/local/bin
	@echo "  - Installing systemd service files to /usr/lib/systemd/system"
	@mkdir -p /usr/lib/systemd/system
	@cp etc/*.service /usr/lib/systemd/system
	@echo "  - You may need to reload systemd daemons: 'systemctl daemon-reload'"
	@echo "  - Done."
