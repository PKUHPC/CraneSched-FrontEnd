package utils

import (
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

var loggerSetupOnce sync.Once

// ensureLoggerDefaults configures logrus with sane defaults once.
func ensureLoggerDefaults() {
	loggerSetupOnce.Do(func() {
		log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
		log.SetLevel(log.InfoLevel)
	})
}

// InitLogger sets the desired log level while keeping other defaults intact.
func InitLogger(level string) {
	ensureLoggerDefaults()

	switch strings.ToLower(level) {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "panic":
		log.SetLevel(log.PanicLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
}
