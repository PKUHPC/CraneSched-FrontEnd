/**
 * Copyright (c) 2025 Peking University and Peking University
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

package ccontrol

import (
	"fmt"
	"os"
	"strconv"
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

type SuspendCommand struct {
	Action        string           `parser:"@'suspend'"`
	ID            string           `parser:"( @String | @Ident | @Number )?"`
	KeyValueParam []*KeyValueParam `parser:"@@*"`
}

type ResumeCommand struct {
	Action        string           `parser:"@'resume'"`
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
	Step        bool `parser:"| @'step'"`
	Reservation bool `parser:"| @'reservation'"`
	Lic         bool `parser:"| @'lic'"`
}

var CControlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "String", Pattern: `[-+]?("[^"]*"|'[^']*'|""|'')`},
	{Name: "Ident", Pattern: `[\+\-]?[a-zA-Z0-9][a-zA-Z0-9_\+\-\.,:@\[\]T]*`},
	{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?`},
	{Name: "Punct", Pattern: `[-=,:]`},
})

var CControlParser = participle.MustBuild[CControlCommand](
	participle.Lexer(CControlLexer),
	participle.Elide("whitespace"),
	participle.Union[any](ShowCommand{}, UpdateCommand{}, HoldCommand{}, ReleaseCommand{}, SuspendCommand{}, ResumeCommand{}, CreateCommand{}, DeleteCommand{}),
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
	case e.Step:
		return "step"
	case e.Reservation:
		return "reservation"
	case e.Lic:
		return "lic"
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
	case SuspendCommand:
		return cmd.Action
	case ResumeCommand:
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
	case SuspendCommand:
		return cmd.ID
	case ResumeCommand:
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
	case SuspendCommand:
		params = cmd.KeyValueParam
	case ResumeCommand:
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
	case SuspendCommand:
		for _, param := range cmd.KeyValueParam {
			kvMap[param.Key] = unquoteIfQuoted(param.Value)
		}
	case ResumeCommand:
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

func getCmdStringByArgs(commandArgs []string) string {
	var processedArgs []string
	for _, arg := range commandArgs {
		if arg == "" {
			processedArgs = append(processedArgs, "\"\"")
			continue
		}

		if strings.Contains(arg, "=") {
			parts := strings.SplitN(arg, "=", 2)
			key := parts[0]
			value := parts[1]

			if value == "" {
				processedArgs = append(processedArgs, key+"=\"\"")
				continue
			}
			if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
				(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
				processedArgs = append(processedArgs, arg)
			} else if strings.Contains(value, " ") {
				processedArgs = append(processedArgs, key+"="+strconv.Quote(value))
			} else {
				processedArgs = append(processedArgs, arg)
			}
		} else if strings.Contains(arg, " ") && !strings.HasPrefix(arg, "'") && !strings.HasPrefix(arg, "\"") {
			processedArgs = append(processedArgs, strconv.Quote(arg))
		} else {
			processedArgs = append(processedArgs, arg)
		}
	}
	cmdStr := strings.Join(processedArgs, " ")
	return cmdStr
}
