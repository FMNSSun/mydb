package mydb

import (
	"io"
	"sync"
	"fmt"
	"os"
)

type DefaultLogger struct {
	w io.Writer
	maxLvl uint8
	prefix string
}

var mutex *sync.Mutex = &sync.Mutex{}

func NewLogger(w io.Writer, prefix string) *DefaultLogger {
	return &DefaultLogger{
		w: w,
		maxLvl: 0xFF,
		prefix: prefix,
	}
}

func (dl *DefaultLogger) Fatalf(msg string, args... interface{}) {
	if dl.w == nil {
		os.Exit(2)
		return
	}

	dl.Outf(LOGLVL_FATAL, msg, args...)
	os.Exit(2)
}

func (dl *DefaultLogger) Fatal(msg string) {
	if dl.w == nil {
		os.Exit(2)
		return
	}

	dl.Out(LOGLVL_FATAL, msg)
	os.Exit(2)
}

func (dl *DefaultLogger) Outf(lvl uint8, msg string, args... interface{}) {
	if dl.w == nil {
		return
	}

	if lvl > dl.maxLvl {
		return
	}

	str := fmt.Sprintf(msg, args)

	dl.Out(lvl, str)
}

func (dl *DefaultLogger) Out(lvl uint8, msg string) {
	if dl.w == nil {
		return
	}

	if lvl > dl.maxLvl {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	str := fmt.Sprintf("%s%s\n", dl.prefix, msg)

	io.WriteString(dl.w, str)
}
