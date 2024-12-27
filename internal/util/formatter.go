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

package util

import (
	"os"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Formatter interface {
	FormatReply(reply interface{}) string
}

// JSON
type FormatterJson struct {
}

var mo = protojson.MarshalOptions{
	EmitDefaultValues: true,
	UseProtoNames:     true,
}

func (f FormatterJson) FormatReply(reply interface{}) string {
	if msg, ok := reply.(protoreflect.ProtoMessage); ok {
		output, err := mo.Marshal(msg)
		if err != nil {
			log.Errorf("Failed to marshal proto message: %v\n", err)
			os.Exit(ErrorInvalidFormat)
		}
		return string(output)
	} else {
		// This should never happen
		log.Errorf("Type %T is not ProtoMessage.\n", reply)
		os.Exit(ErrorInvalidFormat)
	}
	return ""
}

var FmtJson = FormatterJson{}
