#
# Copyright (c) 2024 Peking University and Peking University 
# Changsha Institute for Computing and Digital Economy 
#
# CraneSched is licensed under Mulan PSL v2. 
# You can use this software according to the terms and conditions of 
# the Mulan PSL v2. 
# You may obtain a copy of Mulan PSL v2 at: 
#          http://license.coscl.org.cn/MulanPSL2 
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, 
# WITHOUT WARRANTIES OF ANY KIND, 
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, 
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. 
# See the Mulan PSL v2 for more details. 
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

# Targets
.PHONY: all protos build clean install plugin

all: protos build plugin

protos: 
	@echo "- Generating Protobuf files..."
	@mkdir -p ./generated/protos
	@protoc --go_out=generated --go-grpc_out=generated --proto_path=protos protos/*.proto
	@echo "  - Summary:"
	@echo "    - Protobuf files generated in ./generated/protos/"

build:
	@echo "- Building executables..."
	@mkdir -p $(BIN_DIR)
	@for dir in cmd/*/ ; do \
		echo "  - Building $$dir"; \
		(cd $$dir && go build $(LDFLAGS) -o ../../$(BIN_DIR)/$$(basename $$dir)) || exit 1; \
	done
	@echo "  - Summary:"
	@echo "    - Version: $(VERSION)"
	@echo "    - Build time: $(BUILD_TIME)"
	@echo "    - Commit hash: $(GIT_COMMIT_HASH)"
	@echo "    - Binaries are in ./$(BIN_DIR)/"

plugin:
	@echo "- Building plugins..."
	@mkdir -p $(PLUGIN_DIR)
	@for dir in plugin/*/ ; do \
		echo "  - Building: $$(basename $$dir).so"; \
		(cd $$dir && go build $(LDFLAGS) -buildmode=plugin -o ../../$(PLUGIN_DIR)/$$(basename $$dir).so) || exit 1; \
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
	@echo "  - Installing systemd service files to /etc/systemd/system/"
	@mkdir -p /etc/systemd/system
	@cp etc/*.service /etc/systemd/system
	@echo "  - You may need to reload systemd daemons: 'systemctl daemon-reload'"
	@echo "  - Done."
