package ccontrol

import (
	"fmt"
	"os"
	"strings"

	"CraneFrontEnd/internal/util"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// CControlCommand
type CControlCommand struct {
	Action     *ActionType      `parser:"@@"`
	Resource   *ResourceType    `parser:"@@?"`
	KVParams   []*KeyValueParam `parser:"@@*"`
	GlobeFlags []*Flag          `parser:"@@*"`
}

// KeyValueParam
type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' (@String | @Ident | @TimeFormat | @Number) | (@String | @Ident | @TimeFormat | @Number) )?"`
}

// ActionType
type ActionType struct {
	Show    bool `parser:"@'show'"`
	Update  bool `parser:"| @'update'"`
	Hold    bool `parser:"| @'hold'"`
	Release bool `parser:"| @'release'"`
	Create  bool `parser:"| @'create'"`
	Delete  bool `parser:"| @'delete'"`
}

// ResourceType
type ResourceType struct {
	Node        bool `parser:"@'node'"`
	Partition   bool `parser:"| @'partition'"`
	Job         bool `parser:"| @'job'"`
	Reservation bool `parser:"| @'reservation'"`
}

// Flag
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
)

func ParseCControlCommand(input string) (*CControlCommand, error) {
	return CControlParser.ParseString("", input)
}

func (a ActionType) String() string {
	switch {
	case a.Show:
		return "show"
	case a.Update:
		return "update"
	case a.Hold:
		return "hold"
	case a.Release:
		return "release"
	case a.Create:
		return "create"
	case a.Delete:
		return "delete"
	default:
		return ""
	}
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

// GetAction
func (c *CControlCommand) GetAction() string {
	if c.Action == nil {
		return ""
	}
	return c.Action.String()
}

// GetResource
func (c *CControlCommand) GetResource() string {
	if c.Resource == nil {
		return ""
	}
	return c.Resource.String()
}
// GetKVParamValue
func (c *CControlCommand) GetKVParamValue(key string) string {
	for _, param := range c.KVParams {
		if strings.EqualFold(param.Key, strings.ToLower(key)) {
			return param.Value
		}
	}
	return ""
}

// GetKVMaps
func (c *CControlCommand) GetKVMaps() map[string]string {
	kvMap := make(map[string]string)
	for _, param := range c.KVParams {
		kvMap[param.Key] = param.Value
	}
	return kvMap
}

func (c *CControlCommand) GetGlobalFlag(name string) (string, bool) {
	for _, flag := range c.GlobeFlags {
		if strings.EqualFold(flag.Name, name) {
			return flag.Value, true
		}
	}
	return "", false
}

func (c *CControlCommand) String() string {
	var parts []string

	if c.Action != nil {
		parts = append(parts, c.Action.String())
	}

	if c.Resource != nil {
		parts = append(parts, c.Resource.String())
	}

	for _, flag := range c.GlobeFlags {
		if flag.Value != "" {
			parts = append(parts, fmt.Sprintf("--%s=%s", flag.Name, flag.Value))
		} else {
			parts = append(parts, fmt.Sprintf("--%s", flag.Name))
		}
	}

	for _, kv := range c.KVParams {
		parts = append(parts, fmt.Sprintf("%s=%s", kv.Key, kv.Value))
	}

	return strings.Join(parts, " ")
}

// processGlobalFlags
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
