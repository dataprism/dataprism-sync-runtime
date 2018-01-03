package core

import (
	"time"
	"github.com/lytics/logrus"
)

type DataBuffer struct {
	out chan []Data
	size int

	ticker *time.Ticker

	cache []Data
	offset int
}

func NewDataBuffer(out chan []Data, size int, timeout time.Duration) *DataBuffer {
	return &DataBuffer{ out, size, time.NewTicker(timeout), make([]Data, size), 0}
}

func (b *DataBuffer) Run(done chan int, dataChannel chan Data, errorsChannel chan error) {
	for {
		select {
			case <- done:
				// -- clear what is left in the buffer
				b.rotate()

				// -- stop the ticker
				b.ticker.Stop()

				// -- break out of the loop
				break
			case msg := <- dataChannel:
				// -- add the message to the cache
				b.cache[b.offset] = msg

				// -- increase the offset
				b.offset += 1

				// -- check if a rotation is required
				if b.offset == b.size {
					// -- rotate the buffer
					logrus.Debug("rotating the buffer based on size")
					b.rotate()
				}

			case <- b.ticker.C:
				// -- rotate the buffer
				logrus.Debug("rotating the buffer based on timeout")
				b.rotate()
		}
	}
}

func (b *DataBuffer) rotate() {
	logrus.Debugf("flushing %d data fragments to the output channel", b.offset)
	b.out <- b.cache

	// -- create the new cache buffer
	b.cache = make([]Data, b.size)

	// -- reset the offset
	b.offset = 0
}
