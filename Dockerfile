FROM golang:1.8

RUN cd /tmp && \
    git clone https://github.com/edenhill/librdkafka.git && \
    cd librdkafka && \
    ./configure && \
    make && \
    make install && \
    ldconfig && \
    cd && \
    rm -rf /tmp/librdkafka && \
    mkdir -p /go/src/gitlab.com/vrtoeni/to-kafka

WORKDIR /go/src/gitlab.com/vrtoeni/to-kafka
COPY . .

RUN go-wrapper download   # "go get -d -v ./..."
RUN go-wrapper install    # "go install -v ./..."

CMD ["go-wrapper", "run"] # ["app"]