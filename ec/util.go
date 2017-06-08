//  Copyright (c) 2016 Rackspace

package ec

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

func ParseAddress(address string) (string, int, string, error) {
	parts := strings.Split(address, "/")
	if len(parts) == 2 {
		device := parts[1]
		parts = strings.Split(address, ":")
		if len(parts) == 2 {
			port, err := strconv.Atoi(parts[1])
			if err == nil {
				return parts[0], port, device, nil
			}
		}
	}
	return "", 0, "", errors.New("Invalid node address")
}

// Return the hash for an object.
func ObjHash(vars map[string]string, driveRoot, hashPathPrefix, hashPathSuffix string) string {
	h := md5.New()
	io.WriteString(h, hashPathPrefix+"/"+vars["account"]+"/"+vars["container"]+"/"+vars["obj"]+hashPathSuffix)
	return hex.EncodeToString(h.Sum(nil))
}

// AlignedTick is similar to time.Tick, except it will align itself to duration.  For example, generate a tick every hour, on the hour.
func AlignedTick(duration time.Duration, offset time.Duration) chan struct{} {
	c := make(chan struct{})
	d := int64(duration)
	go func() {
		for {
			now := time.Now()
			time.Sleep(time.Duration(d-(now.UnixNano()%d)) + offset)
			c <- struct{}{}
		}
	}()
	return c
}

// ECSplit takes a Reader, chunks it by chunkSize, and writes EC fragments to a list of writers.
func ECSplit(dataChunks, parityChunks int, fp io.Reader, chunkSize int, contentLength int64, writers []io.Writer) error {
	enc, err := reedsolomon.New(dataChunks, parityChunks)
	if err != nil {
		return err
	}
	data := make([][]byte, dataChunks+parityChunks)
	databuf := make([]byte, (dataChunks+parityChunks)*chunkSize)
	for i := range data {
		data[i] = databuf[i*chunkSize : (i+1)*chunkSize]
	}
	totalRead := int64(0)
	wg := sync.WaitGroup{}
	for totalRead < contentLength {
		expectedRead := dataChunks * chunkSize
		if contentLength-totalRead < int64(expectedRead) {
			expectedRead = int(contentLength - totalRead)
		}
		read, _ := io.ReadFull(fp, databuf[:expectedRead])
		if read == 0 {
			return errors.New("Short read")
		}
		totalRead += int64(read)
		for read%dataChunks != 0 { // pad data with 0s to a multiple of dataChunks
			databuf[read] = 0
			read++
		}
		thisChunkSize := read / dataChunks
		for i := range data { // assign data chunks
			data[i] = databuf[i*thisChunkSize : (i+1)*thisChunkSize]
		}
		if err := enc.Encode(data); err != nil {
			return err
		}
		for i := range data {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				writers[i].Write(data[i])
			}(i)
		}
		wg.Wait()
	}
	return nil
}

// ECGlue takes readers of EC fragments and un-ECs them, writing the chunks to dsts.
func ECGlue(dataChunks, parityChunks int, bodies []io.Reader, chunkSize int, contentLength int64, dsts ...io.Writer) error {
	enc, err := reedsolomon.New(dataChunks, parityChunks)
	if err != nil {
		return err
	}
	data := make([][]byte, dataChunks+parityChunks)
	databuf := make([]byte, (dataChunks+parityChunks)*chunkSize)
	totalWritten := int64(0)
	wg := sync.WaitGroup{}
	for totalWritten < contentLength {
		expectedChunkSize := chunkSize
		if contentLength-totalWritten < int64(chunkSize*dataChunks) {
			expectedChunkSize = int((contentLength - totalWritten) / int64(dataChunks))
			if (contentLength-totalWritten)%int64(dataChunks) != 0 {
				expectedChunkSize++
			}
		}
		for i := range data {
			if bodies[i] != nil {
				data[i] = databuf[i*expectedChunkSize : (i+1)*expectedChunkSize]
			}
		}
		for i := range bodies {
			if bodies[i] != nil {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					if _, err := io.ReadFull(bodies[i], data[i]); err != nil {
						data[i] = nil
						bodies[i] = nil
					}
				}(i)
			}
		}
		wg.Wait()
		if err := enc.Reconstruct(data); err != nil {
			return err
		}
		for i := 0; i < dataChunks; i++ {
			if contentLength-totalWritten < int64(len(data[i])) { // strip off any padding
				data[i] = data[i][:contentLength-totalWritten]
			}
			wg.Add(1)
			for _, d := range dsts {
				go func(i int, d io.Writer) {
					defer wg.Done()
					d.Write(data[i])
				}(i, d)
			}
			wg.Wait()
			totalWritten += int64(len(data[i]))
		}
	}
	return nil
}
