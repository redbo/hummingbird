//  Copyright (c) 2016 Rackspace

package ec

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestECRoundtrip(t *testing.T) {
	testECRoundTrip := func(data string, dataChunks int, parityChunks int, chunkSize int) {
		testData := bytes.NewBufferString(data)
		testOut := &bytes.Buffer{}
		readers := make([]io.Reader, dataChunks+parityChunks)
		writers := make([]io.Writer, dataChunks+parityChunks)
		for i := 0; i < dataChunks+parityChunks; i++ {
			rp, wp := io.Pipe()
			defer rp.Close()
			defer wp.Close()
			readers[i] = rp
			writers[i] = wp
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			require.Nil(t, ECSplit(dataChunks, parityChunks, testData, chunkSize, int64(len(data)), writers))
			wg.Done()
		}()
		require.Nil(t, ECGlue(dataChunks, parityChunks, readers, chunkSize, int64(len(data)), testOut))
		wg.Wait()
		require.Equal(t, data, testOut.String())
	}

	alpha := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for i := 0; i < len(alpha); i++ {
		for chunkSize := 1; chunkSize < 10; chunkSize++ {
			for dataChunks := 3; dataChunks < 10; dataChunks++ {
				for parityChunks := 1; parityChunks < 6; parityChunks++ {
					testECRoundTrip(alpha[:i], dataChunks, parityChunks, chunkSize)
				}
			}
		}
	}
}
