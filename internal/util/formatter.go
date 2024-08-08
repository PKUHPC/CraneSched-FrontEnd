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
		output, _ := mo.Marshal(msg)
		return string(output)
	} else {
		log.Fatalf("Type %T is not ProtoMessage.\n", reply)
		os.Exit(ErrorInvalidFormat)
	}
	return ""
}

var FmtJson = FormatterJson{}
