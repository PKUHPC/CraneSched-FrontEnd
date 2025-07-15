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
	Action string      `parser:"@'show'"`
	Entity *EntityType `parser:"@@"`
	ID     string      `parser:"( @String | @Ident | @Number )?"`
}

type UpdateCommand struct {
	Action        string           `parser:"@'update'"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}

type HoldCommand struct {
	Action        string           `parser:"@'hold'"`
	ID            string           `parser:"( @String | @Ident | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}

type ReleaseCommand struct {
	Action        string           `parser:"@'release'"`
	ID            string           `parser:"( @String | @Ident | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}

type CreateCommand struct {
	Action        string           `parser:"@'create'"`
	Entity        *EntityType      `parser:"@@"`
	ID            string           `parser:"( @String | @Ident | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}

type DeleteCommand struct {
	Action string      `parser:"@'delete'"`
	Entity *EntityType `parser:"@@"`
	ID     string      `parser:"( @String | @Ident | @Number )?"`
}

type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' ( @String | @Ident | @Number ) | ( @String | @Ident | @Number ) )"`
}

type EntityType struct {
	Node        bool `parser:"@'node'"`
	Partition   bool `parser:"| @'partition'"`
	Job         bool `parser:"| @'job'"`
	Reservation bool `parser:"| @'reservation'"`
}

var CControlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "String", Pattern: `[-+]?("[^"]*"|'[^']*'|""|'')`},
	{Name: "Ident", Pattern: `[\+\-]?[a-zA-Z0-9][a-zA-Z0-9_\+\-\.,:\[\]T]*`},
	{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?`},
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
	case CreateCommand:
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
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	case HoldCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	case ReleaseCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	case CreateCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	}
	return kvMap
}

func preParseGlobalFlags(args []string) []string {
	remainingArgs := []string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var flagName, flagValue string
		var hasValueInSameArg bool

		if strings.HasPrefix(arg, "-") {
			if strings.Contains(arg, "=") {
				parts := strings.SplitN(arg, "=", 2)
				flagName = parts[0]
				flagValue = parts[1]
				hasValueInSameArg = true
			} else {
				flagName = arg
			}
		} else {
			remainingArgs = append(remainingArgs, arg)
			continue
		}

		switch flagName {
		case "-h", "--help":
			showHelp()
			os.Exit(0)
		case "-v", "--version":
			fmt.Println(util.Version())
			os.Exit(0)
		case "-J", "--json":
			FlagJson = true
		case "-C", "--config":
			if hasValueInSameArg {
				FlagConfigFilePath = flagValue
			} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				FlagConfigFilePath = args[i+1]
				i++
			}
		default:
			remainingArgs = append(remainingArgs, arg)
		}
	}
	return remainingArgs
}

func unquoteIfQuoted(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
