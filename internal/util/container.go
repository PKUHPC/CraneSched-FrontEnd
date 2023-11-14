package util

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"os"
	"os/user"
	"path"
	"strings"
)

type ContainerConfig struct {
	RuntimeState  string `yaml:"runtimeState"`
	RuntimeRun    string `yaml:"runtimeRun"`
	RuntimeKill   string `yaml:"runtimeKill"`
	RuntimeDelete string `yaml:"runtimeDelete"`
}

func GetOCIRunCmd(bundlePath string, configPath string) (string, error) {
	config := ParseConfig(configPath)
	runCmd := config.Container.RuntimeRun

	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}

	// Iterate over the map and replace each placeholder
	replacements := map[string]string{
		"%%": "%",
		"%b": bundlePath,            // path to bundle
		"%c": path.Base(bundlePath), // container name
		"%u": currentUser.Username,  // username
		"%U": currentUser.Uid,       // userid
	}
	for key, value := range replacements {
		runCmd = strings.ReplaceAll(runCmd, key, value)
	}

	return runCmd, nil
}

func ValidateOCIBundlePath(bundlePath string) (ok bool, err error) {
	// Check if path exists
	ok = false
	if _, err = os.Stat(bundlePath); err != nil {
		return
	}

	// Check if there is a rootfs and a config.json
	if _, err = os.Stat(path.Join(bundlePath, "rootfs")); err != nil {
		return
	}

	if _, err = os.Stat(path.Join(bundlePath, "config.json")); err != nil {
		return
	}

	return true, nil
}

func ValidateOCIConfig(bundlePath string) (ok bool, err error) {
	ok = false
	configPath := path.Join(bundlePath, "config.json")

	file, err := os.ReadFile(configPath)
	if err != nil {
		return
	}

	if !gjson.ValidBytes(file) {
		err = errors.New("Invalid JSON format")
		return
	}

	if !gjson.GetBytes(file, "process.terminal").IsBool() || gjson.GetBytes(file, "process.terminal").Bool() {
		err = errors.New("process.terminal must be set to false")
		return
	}

	if !gjson.GetBytes(file, "process.args").IsArray() {
		err = errors.New("process.args must be an array")
		return
	}
	// TODO: Add more validation...

	return true, nil
}

func WriteContainerScript(bundlePath string, scriptName string, script *[]string) error {
	_, err := os.Stat(bundlePath)
	if err != nil {
		return err
	}

	scriptPath := path.Join(path.Join(bundlePath, "rootfs"), path.Base(scriptName))
	file, err := os.Create(scriptPath)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err = f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)

	_, err = file.WriteString(strings.Join(*script, "\n"))
	return err
}
