package ccontrol

import (
	"fmt"
	"os"
	"strings"

	"CraneFrontEnd/internal/util"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type CControlCommand struct {
	Command any `parser:"@@"`
}

type ShowCommand struct {
	Action      string      `parser:"@'show'"`
	Entity      *EntityType `parser:"@@"`
	ID          string      `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	GlobalFlags []*Flag     `parser:"@@*"`
}

type UpdateCommand struct {
	Action        string           `parser:"@'update'"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobalFlags   []*Flag          `parser:"@@*"`
}

type HoldCommand struct {
	Action        string           `parser:"@'hold'"`
	ID            string           `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobalFlags   []*Flag          `parser:"@@*"`
}

type ReleaseCommand struct {
	Action        string           `parser:"@'release'"`
	ID            string           `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobalFlags   []*Flag          `parser:"@@*"`
}

type CreateCommand struct {
	Action        string           `parser:"@'create'"`
	Entity        *EntityType      `parser:"@@"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobalFlags   []*Flag          `parser:"@@*"`
}

type DeleteCommand struct {
	Action      string      `parser:"@'delete'"`
	Entity      *EntityType `parser:"@@"`
	ID          string      `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	GlobalFlags []*Flag     `parser:"@@*"`
}

type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
}

type EntityType struct {
	Node        bool `parser:"@'node'"`
	Partition   bool `parser:"| @'partition'"`
	Job         bool `parser:"| @'job'"`
	Reservation bool `parser:"| @'reservation'"`
}

type Flag struct {
	Name  string `parser:"'-' '-'? @Ident"`
	Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
}

var CControlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "String", Pattern: `"[^"]*"|'[^']*'`},
	{Name: "TimeFormat", Pattern: `\d+:\d+:\d+|\d+-\d+:\d+:\d+`},
	{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?`},
	{Name: "Ident", Pattern: `[a-zA-Z][a-zA-Z0-9_\-\.,]*`},
	{Name: "Punct", Pattern: `[-=,:]`},
})

var CControlParser = participle.MustBuild[CControlCommand](
	participle.Lexer(CControlLexer),
	participle.Elide("whitespace"),
	participle.Union[any](ShowCommand{}, UpdateCommand{}, HoldCommand{}, ReleaseCommand{}, CreateCommand{}, DeleteCommand{}),
)

func ParseCControlCommand(input string) (*CControlCommand, error) {
	return CControlParser.ParseString("", input)
}

func (e EntityType) String() string {
	switch {
	case e.Node:
		return "node"
	case e.Partition:
		return "partition"
	case e.Job:
		return "job"
	case e.Reservation:
		return "reservation"
	default:
		return ""
	}
}

func (c *CControlCommand) GetAction() string {
	switch cmd := c.Command.(type) {
	case ShowCommand:
		return cmd.Action
	case UpdateCommand:
		return cmd.Action
	case HoldCommand:
		return cmd.Action
	case ReleaseCommand:
		return cmd.Action
	case CreateCommand:
		return cmd.Action
	case DeleteCommand:
		return cmd.Action
	default:
		return ""
	}
}

func (c *CControlCommand) GetEntity() string {
	switch cmd := c.Command.(type) {
	case ShowCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case CreateCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case DeleteCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	}
	return ""
}

func (c *CControlCommand) GetID() string {
	switch cmd := c.Command.(type) {
	case ShowCommand:
		return cmd.ID
	case HoldCommand:
		return cmd.ID
	case ReleaseCommand:
		return cmd.ID
	case DeleteCommand:
		return cmd.ID
	}
	return ""
}

func (c *CControlCommand) GetKVParamValue(key string) string {
	var params []*KeyValueParam
	switch cmd := c.Command.(type) {
	case UpdateCommand:
		params = cmd.KeyValueParam
	case HoldCommand:
		params = cmd.KeyValueParam
	case ReleaseCommand:
		params = cmd.KeyValueParam
	case CreateCommand:
		params = cmd.KeyValueParam
	}

	for _, param := range params {
		if strings.EqualFold(param.Key, key) {
			return param.Value
		}
	}
	return ""
}

func (c *CControlCommand) GetKVMaps() map[string]string {
	kvMap := make(map[string]string)
	switch cmd := c.Command.(type) {
	case UpdateCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = param.Value
		}
	case HoldCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = param.Value
		}
	case ReleaseCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = param.Value
		}
	case CreateCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = param.Value
		}
	}
	return kvMap
}

func (c *CControlCommand) GetGlobalFlag(name string) (string, bool) {
	var flags []*Flag
	switch cmd := c.Command.(type) {
	case ShowCommand:
		flags = cmd.GlobalFlags
	case UpdateCommand:
		flags = cmd.GlobalFlags
	case HoldCommand:
		flags = cmd.GlobalFlags
	case ReleaseCommand:
		flags = cmd.GlobalFlags
	case CreateCommand:
		flags = cmd.GlobalFlags
	case DeleteCommand:
		flags = cmd.GlobalFlags
	}
	for _, flag := range flags {
		if strings.EqualFold(flag.Name, name) {
			return flag.Value, true
		}
	}
	return "", false
}

func processGlobalFlags(command *CControlCommand) {
	_, hasJson := command.GetGlobalFlag("json")
	_, hasJ := command.GetGlobalFlag("J")
	if hasJson || hasJ {
		FlagJson = true
	}

	configFilePath, hasConfig := command.GetGlobalFlag("config")
	configFilePathShort, hasC := command.GetGlobalFlag("C")
	if hasConfig {
		FlagConfigFilePath = configFilePath
	} else if hasC {
		FlagConfigFilePath = configFilePathShort
	}

	_, hasHelp := command.GetGlobalFlag("help")
	_, hasH := command.GetGlobalFlag("h")
	if hasHelp || hasH {
		showHelp()
		os.Exit(0)
	}

	_, hasVersion := command.GetGlobalFlag("version")
	_, hasV := command.GetGlobalFlag("v")
	if hasVersion || hasV {
		fmt.Println(util.Version())
		os.Exit(0)
	}
}
