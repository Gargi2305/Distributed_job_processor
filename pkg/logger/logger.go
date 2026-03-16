package logger

import (
	"log"
	"os"
)

var (
	infoLogger  = log.New(os.Stdout, "INFO ", log.LstdFlags|log.Lmicroseconds)
	errorLogger = log.New(os.Stderr, "ERROR ", log.LstdFlags|log.Lmicroseconds)
)

func Info(event string, msg string) {
	infoLogger.Printf("%s %s", event, msg)
}

func Error(event string, msg string) {
	errorLogger.Printf("%s %s", event, msg)
}
