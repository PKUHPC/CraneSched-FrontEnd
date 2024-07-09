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

# Build with protobuf and version injection

# Variables
GIT_COMMIT_HASH := $(shell git rev-parse --short HEAD)
VERSION_FILE := VERSION
VERSION := $(shell [ -f $(VERSION_FILE) ] && cat $(VERSION_FILE) || echo $(GIT_COMMIT_HASH))
BUILD_TIME := $(shell date +'%a, %d %b %Y %H:%M:%S %z')
LDFLAGS := -ldflags \
			"-X 'CraneFrontEnd/internal/util.VERSION=$(VERSION)' \
			-X 'CraneFrontEnd/internal/util.BUILD_TIME=$(BUILD_TIME)'"
BIN_DIR := bin

# Targets
.PHONY: all protos build clean install

all: protos build

protos: 
	@echo "Generating Protobuf files..."
	@mkdir -p ./generated/protos
	@protoc --go_out=generated --go-grpc_out=generated --proto_path=protos protos/*.proto

build:
	@echo "Building CraneSched Frontend with version $(VERSION)..."
	@mkdir -p $(BIN_DIR)
	@for dir in cmd/*/ ; do \
		echo "  Building $$dir"; \
		(cd $$dir && go build $(LDFLAGS) -o ../../$(BIN_DIR)/$$(basename $$dir)) || exit 1; \
	done
	@echo "============ Build Summary ============"
	@echo "  Version: $(VERSION)"
	@echo "  Build time: $(BUILD_TIME)"
	@echo "  Commit hash: $(GIT_COMMIT_HASH)"
	@echo "  Binaries are in ./$(BIN_DIR)/ ,"
	@echo "  use 'make install' to install them. "
	@echo "======================================="

clean:
	@echo "Cleaning up..."
	@rm -rf $(BIN_DIR)

install: 
	@if [ ! -d "$(BIN_DIR)" ]; then \
		echo "Error: $(BIN_DIR) does not exist. Please build the binaries first."; \
		exit 1; \
	fi
	@echo "Installing binaries to /usr/local/bin/"
	@mkdir -p /usr/local/bin
	@cp $(BIN_DIR)/* /usr/local/bin
	@echo "Done."
