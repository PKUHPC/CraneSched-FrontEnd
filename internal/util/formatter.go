package util

import (
	"os"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Formatter interface {
	Name() string
	FormatReply(reply interface{}) string
}

// JSON

type formatterJSON struct {
}

func (f formatterJSON) Name() string {
	return "json"
}

var mo = protojson.MarshalOptions{
	EmitDefaultValues: true,
	UseProtoNames:     true,
}

func (f formatterJSON) FormatReply(reply interface{}) string {
	if msg, ok := reply.(protoreflect.ProtoMessage); ok {
		output, _ := mo.Marshal(msg)
		return string(output)
	} else {
		log.Fatalf("Type %T is not ProtoMessage.\n", reply)
		os.Exit(ErrorInvalidFormat)
	}
	return ""
}

var FormatterJSON = formatterJSON{}
