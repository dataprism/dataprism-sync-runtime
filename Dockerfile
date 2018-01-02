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
    mkdir -p /go/src/github.com/dataprism/dataprism-sync-runtime

WORKDIR /go/src/github.com/dataprism/dataprism-sync-runtime
COPY . .

RUN go-wrapper download   # "go get -d -v ./..."
RUN go-wrapper install    # "go install -v ./..."

CMD ["go-wrapper", "run"] # ["app"]