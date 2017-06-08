//  Copyright (c) 2016 Rackspace

package ec

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/objectserver"
)

const notFound = 0
const inNursery = 1
const inOFH = 2

type HeckObject struct {
	*HeckObjectFactory
	file         *os.File
	location     int
	hash         string
	nurseryPath  string
	ofhPath      string
	workingClass string
	tempFile     string
	metadata     map[string]string
}

var _ objectserver.Object = &HeckObject{}

func (o *HeckObject) Metadata() map[string]string {
	return o.metadata
}

func (o *HeckObject) Audit() error {
	return nil
}

func (o *HeckObject) ContentLength() int64 {
	if contentLength, err := strconv.ParseInt(o.metadata["Content-Length"], 10, 64); err != nil {
		return -1
	} else {
		return contentLength
	}
}

func (o *HeckObject) Quarantine() error {
	return errors.New("Unimplemented")
}

func (o *HeckObject) Exists() bool {
	if o.location == notFound {
		return false
	}
	if metadata := o.Metadata(); metadata == nil || hummingbird.LooksTrue(metadata["Deleted"]) {
		return false
	}
	return true
}

func (o *HeckObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if o.location == inNursery {
		dataFile, _ := objectserver.ObjectFiles(o.nurseryPath)
		if strings.HasPrefix(dataFile, ".data") {
			if f, err := os.Open(dataFile); err != nil {
				return 0, errors.New("Error opening file")
			} else {
				defer f.Close()
				return hummingbird.Copy(f, dsts...)
			}
		} else {
			return 0, errors.New("Data file disappeared")
		}
	} else if o.location == inOFH {
		parts := strings.Split(o.metadata["HeckPolicy"], "/")
		if len(parts) != 2 {
			return 0, errors.New("Invalid Heck policy")
		}
		dataChunks, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, errors.New("Invalid Heck data chunks")
		}
		parityChunks, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, errors.New("Invalid Heck parity chunks")
		}
		contentLength := o.ContentLength()
		val, err := strconv.ParseUint(o.hash[0:8], 16, 64)
		if err != nil {
			return 0, errors.New("Invalid object hash")
		}
		partition := uint32(val >> (64 - o.Ring.PartitionBitCount()))
		nodes := o.Ring.ResponsibleNodes(partition)
		if len(nodes) < dataChunks+parityChunks {
			return 0, errors.New("Not enough nodes for Heck policy")
		}
		bodies := make([]io.Reader, len(nodes))
		chunkSize, err := strconv.Atoi(o.metadata["ChunkSize"])
		if err != nil {
			return 0, errors.New("Invalid chunk size")
		}

		for _, node := range nodes {
			host, port, device, err := ParseAddress(node.Address(0))
			if err != nil {
				continue
			}
			resp, err := o.client.Get(fmt.Sprintf("http://%s:%d/ec-frag/%s/%s", host, port, device, o.hash))
			if err != nil {
				continue
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				continue
			}
			index, err := strconv.Atoi(resp.Header.Get("FragIndex"))
			if err != nil {
				continue
			}
			bodies[index] = resp.Body
		}
		ECGlue(dataChunks, parityChunks, bodies, chunkSize, contentLength, dsts...)
	}
	return 0, errors.New("Object doesn't exist")
}

func (o *HeckObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	return 0, errors.New("Unimplemented")
}

func (o *HeckObject) Repr() string {
	return fmt.Sprintf("HeckObject(%s)", o.hash)
}

func (o *HeckObject) newFile(class string, size int64) (io.Writer, error) {
	o.Close()
	tempDir := filepath.Join(o.nurseryPath, "../../../..", "tmp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("Unable to create temp directory: %v", err)
	}
	f, err := ioutil.TempFile(tempDir, "ecoe")
	if err != nil {
		return nil, fmt.Errorf("Error creating temp file: %v", err)
	}
	if freeSpace, err := objectserver.FreeDiskSpace(f.Fd()); err != nil {
		f.Close()
		return nil, fmt.Errorf("Unable to stat filesystem: %v", err)
	} else if o.reserve > 0 && freeSpace-size < o.reserve {
		f.Close()
		return nil, objectserver.DriveFullError
	}
	if size > 0 {
		syscall.Fallocate(int(f.Fd()), 1, 0, size)
	}
	o.workingClass = class
	o.file = f
	o.tempFile = f.Name()
	return f, nil
}

func (o *HeckObject) SetData(size int64) (io.Writer, error) {
	return o.newFile("data", size)
}

func (o *HeckObject) Commit(metadata map[string]string) error {
	defer o.file.Close()
	timestamp, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("No timestamp in metadata")
	}
	fileName := filepath.Join(o.nurseryPath, fmt.Sprintf("%s.%s", timestamp, o.workingClass))
	if err := objectserver.WriteMetadata(o.file.Fd(), metadata); err != nil {
		return fmt.Errorf("Error writing metadata: %v", err)
	}
	o.file.Sync()
	if os.MkdirAll(o.nurseryPath, 0755) != nil || os.Rename(o.file.Name(), fileName) != nil {
		return fmt.Errorf("Error renaming temporary file: %s -> %s", o.file.Name(), fileName)
	}
	go func() {
		objectserver.HashCleanupListDir(o.nurseryPath)
		if dir, err := os.OpenFile(o.nurseryPath, os.O_RDONLY, 0666); err == nil {
			dir.Sync()
			dir.Close()
		}
		objectserver.InvalidateHash(o.nurseryPath)
	}()
	return nil
}

func (o *HeckObject) Delete(metadata map[string]string) error {
	if _, err := o.newFile("ts", 0); err != nil {
		return err
	} else {
		defer o.Close()
		return o.Commit(metadata)
	}
}

func (o *HeckObject) Close() error {
	if o.tempFile != "" {
		defer os.Remove(o.tempFile)
		o.tempFile = ""
	}
	if o.file != nil {
		defer o.file.Close()
		o.file = nil
	}
	return nil
}
