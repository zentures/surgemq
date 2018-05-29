package commons

import (
	"log"
	"os"

	"go.uber.org/zap"
)

var (
	SystemDebug bool
	Log         *zap.Logger
)

func init() {
	var err error

	if os.Getenv("SURGEMQ_DEBUG") == "1" {
		SystemDebug = true
	} else {
		SystemDebug = false
	}

	if !SystemDebug {
		Log, err = zap.NewProduction()
	} else {
		Log, err = zap.NewDevelopment()
	}

	if err != nil {
		log.Fatal(err)
	}
}
