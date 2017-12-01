#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

docker run --rm -v "$SCRIPTPATH":/go/src/gitlab.com/vrtoeni/to-kafka -w /go/src/gitlab.com/vrtoeni/to-kafka -e GOOS=linux -e GOARCH=amd64 golang:1.8 go get ./... && go build -v