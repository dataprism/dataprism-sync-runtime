package s3

import (
    "bytes"
    "github.com/rlmcpherson/s3gof3r"
    "time"
    "io"
    "runtime"
    "encoding/csv"
    "github.com/dataprism/dataprism-sync-runtime/core"
    "github.com/armon/go-metrics"
    "github.com/sirupsen/logrus"
)

type S3Bucket struct {
    Region string
    Name string
    In chan core.Data
}

func PrintMemStats() {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    logrus.Debugf("Heap System: %dMb, Heap allocated: %dMb, Heap Idle: %dMb, Heap Released: %dMb", m.HeapSys / 1000 / 1000, m.HeapAlloc / 1000 / 1000,
        m.HeapIdle / 1000 / 1000, m.HeapReleased / 1000 / 1000)


    //metrics.LogIntegerGauge("output.s3.heap_allocated", int64(m.HeapAlloc))
    //metrics.LogIntegerGauge("output.s3.heap_idle", int64(m.HeapIdle))
    //metrics.LogIntegerGauge("output.s3.heap_allocated", int64(m.HeapReleased))
}

func NewBucket(region string, path string, name string, lineCount int) (*S3Bucket, error) {
    // input chan core.Data --> buffer chan *bytes.Buffer --> write to s3
    inputCh := make(chan core.Data)
    bufferCh := Chunk(inputCh, lineCount)

    go BufferToS3(name, path, region, bufferCh)

    return &S3Bucket{
        Region: region,
        Name: name,
        In: inputCh,
    }, nil
}

func Chunk(in chan core.Data, lineCount int) (chan *bytes.Buffer){
    out := make(chan *bytes.Buffer)

    go func() {
        var b = new(bytes.Buffer)
        csv := csv.NewWriter(b)

        lines := make([][]string, lineCount)
        currentLine := 0

        c := true

        for c {
            select{
            case data := <-in:
                // write incoming bytes to buffer
                columns := [2]string{"key", "value"}

                key := string(data.GetKey())
                val := string(data.GetValue())

                logrus.Println(val)

                line := make([]string, len(columns))
                line[0] = key
                line[1] = val

                lines[currentLine] = line
                currentLine++

                if currentLine >= lineCount {
                    csv.WriteAll(lines) // flush is implicit

                    // -- this works, but creates a new buffer for each chunk, causing GC stress
                    var bCopy = new(bytes.Buffer)
                    io.Copy(bCopy, b)

                    b.Reset()

                    lines = make([][]string, lineCount)
                    currentLine = 0

                    PrintMemStats(metrics)

                    out <- bCopy

                    metrics.GetMetricChan() <- core.NewMetricIncrement("output.s3.chunks_buffered", 1)
                }

            }

        }
    }()
    return out
}

func BufferToS3(bucketName string, bucketPath string, region string, buffersIn chan *bytes.Buffer) {
    // attempt to fetch env keys
    keys, err := s3gof3r.EnvKeys()

    if err != nil {
        logrus.Errorf("failed to get aws keys %v", err)
        return
    }

    // open connection to bucket
    s3 := s3gof3r.New(region, keys)
    bucket := s3.Bucket(bucketName)

    stop := false

    logrus.Debugf("listening to buffer channel.. ")

    for !stop {
        buffer := <-buffersIn

        logrus.Debugf("writing to s3...")

        // open write stream
        writer, err := bucket.PutWriter(bucketPath + "/" + time.Now().Format("2006-01-02/15-04-05") + ".csv", nil,nil)
        if err != nil {
            logrus.Errorf("failed to open connection to bucket %v", err)
            continue
        }

        // copy buffer to writer
        if _, err := io.Copy(writer, buffer) ; err != nil {
            logrus.Warnf("failed to copy buffer to s3 writer")
            continue
        }

        // close connection
        if err = writer.Close(); err != nil {
            logrus.Errorf("failed to close s3 writer connection %v", err)
            metrics.GetMetricChan() <- core.NewMetricIncrement("output.s3.chunks_failed", 1)
        } else {
            logrus.Debugf(".. closed connection to s3")
            metrics.GetMetricChan() <- core.NewMetricIncrement("output.s3.chunks_written", 1)
        }
    }

}
