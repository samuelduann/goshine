package goshine

import (
	"log"
	"os"
)

var (
	errLog Logger = log.New(os.Stderr, "[GOSHINE] ", log.Ldate|log.Ltime|log.Lshortfile)
)

type Logger interface {
	Printf(format string, v ...interface{})
}
