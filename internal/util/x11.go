package util

import (
	"bytes"
	"errors"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"regexp"
)

func X11GetXAuthCookie() (string, error) {
	const cookiePattern = `(?m)[a-zA-Z0-9./_-]+:[0-9]+\s+MIT-MAGIC-COOKIE-1\s+([0-9a-fA-F]+)$`
	const wildcardPattern = `(?m)#ffff#[0-9a-fA-F./_-]+#:[0-9]+\s+MIT-MAGIC-COOKIE-1\s+([0-9a-fA-F]+)$`

	display := os.Getenv("DISPLAY")
	if display == "" {
		return "", errors.New("DISPLAY environment variable not set")
	}

	cmd := exec.Command("xauth", "list", display)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Debugf("Error running xauth command: %v. Stderr: %s", err, stderr.String())
		return "", errors.New("cannot use X11 forwarding")
	}

	log.Debugf("Get xauth list $DISPLAY: %s", stdout.String())

	result := stdout.String()
	cookieRegex := regexp.MustCompile(cookiePattern)
	wildcardRegex := regexp.MustCompile(wildcardPattern)

	match := cookieRegex.FindStringSubmatch(result)
	if match == nil {
		log.Debugf("Could not retrieve magic cookie, checking for wildcard cookie.")
		match = wildcardRegex.FindStringSubmatch(result)
		if match == nil {
			return "", errors.New("could not retrieve magic cookie, cannot use X11 forwarding")
		}
	}

	if len(match) < 2 {
		return "", errors.New("invalid match format for magic cookie")
	}

	return match[1], nil
}
