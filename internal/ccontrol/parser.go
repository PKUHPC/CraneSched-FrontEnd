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
	Action     string        `parser:"@'show'"`
	Resource   *ResourceType `parser:"@@"`
	ID         string        `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	GlobeFlags []*Flag       `parser:"@@*"`
}

type UpdateCommand struct {
	Action        string           `parser:"@'update'"`
	Resource      *ResourceType    `parser:"@@"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobeFlags    []*Flag          `parser:"@@*"`
}

type HoldCommand struct {
	Action        string           `parser:"@'hold'"`
	Resource      *ResourceType    `parser:"@@"`
	ID            string           `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobeFlags    []*Flag          `parser:"@@*"`
}

type ReleaseCommand struct {
	Action        string           `parser:"@'release'"`
	Resource      *ResourceType    `parser:"@@"`
	ID            string           `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobeFlags    []*Flag          `parser:"@@*"`
}

type CreateCommand struct {
	Action        string           `parser:"@'create'"`
	Resource      *ResourceType    `parser:"@@"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
	GlobeFlags    []*Flag          `parser:"@@*"`
}

type DeleteCommand struct {
	Action     string        `parser:"@'delete'"`
	Resource   *ResourceType `parser:"@@"`
	ID         string        `parser:"( @String | @Ident | @TimeFormat | @Number )?"`
	GlobeFlags []*Flag       `parser:"@@*"`
}

type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
}

type ResourceType struct {
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

func (r ResourceType) String() string {
	switch {
	case r.Node:
		return "node"
	case r.Partition:
		return "partition"
	case r.Job:
		return "job"
	case r.Reservation:
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

func (c *CControlCommand) GetResource() string {
	switch cmd := c.Command.(type) {
	case ShowCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case UpdateCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case HoldCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case ReleaseCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case CreateCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
		}
	case DeleteCommand:
		if cmd.Resource != nil {
			return cmd.Resource.String()
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
	switch cmd := c.Command.(type) {
	case UpdateCommand:
		for _, param := range cmd.KeyValueParam {
			if strings.EqualFold(param.Key, strings.ToLower(key)) {
				return param.Value
			}
		}
	case HoldCommand:
		for _, param := range cmd.KeyValueParam {
			if strings.EqualFold(param.Key, strings.ToLower(key)) {
				return param.Value
			}
		}
	case ReleaseCommand:
		for _, param := range cmd.KeyValueParam {
			if strings.EqualFold(param.Key, strings.ToLower(key)) {
				return param.Value
			}
		}
	case CreateCommand:
		for _, param := range cmd.KeyValueParam {
			if strings.EqualFold(param.Key, strings.ToLower(key)) {
				return param.Value
			}
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
	switch cmd := c.Command.(type) {
	case ShowCommand:
		for _, flag := range cmd.GlobeFlags {
			if strings.EqualFold(flag.Name, name) {
				return flag.Value, true
			}
		}
	case UpdateCommand:
		for _, flag := range cmd.GlobeFlags {
			if strings.EqualFold(flag.Name, name) {
				return flag.Value, true
			}
		}
	case HoldCommand:
		for _, flag := range cmd.GlobeFlags {
			if strings.EqualFold(flag.Name, name) {
				return flag.Value, true
			}
		}
	case ReleaseCommand:
		for _, flag := range cmd.GlobeFlags {
			if strings.EqualFold(flag.Name, name) {
				return flag.Value, true
			}
		}
	case CreateCommand:
		for _, flag := range cmd.GlobeFlags {
			if strings.EqualFold(flag.Name, name) {
				return flag.Value, true
			}
		}
	case DeleteCommand:
		for _, flag := range cmd.GlobeFlags {
			if strings.EqualFold(flag.Name, name) {
				return flag.Value, true
			}
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
