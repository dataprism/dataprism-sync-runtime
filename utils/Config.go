package utils

import (
	"os"
	"strings"
	"github.com/sirupsen/logrus"
)

func ParseEnvVars() map[string]string {
	config := make(map[string]string, len(os.Environ()))

	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")
		config[pair[0]] = pair[1]
		logrus.Debug(pair[0] + " = " + pair[1])
	}

	return config
}