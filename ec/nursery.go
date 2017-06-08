//  Copyright (c) 2016 Rackspace

package ec

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gholt/ring"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/objectserver"
)

func (f *HeckObjectFactory) localDevices() ([]string, error) {
	var localIPs = make(map[string]bool)

	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range localAddrs {
		localIPs[strings.Split(addr.String(), "/")[0]] = true
	}

	localDevices := make([]string, 0)
	for _, node := range f.Ring.Nodes() {
		host, port, device, err := ParseAddress(node.Address(0))
		if err != nil {
			continue
		}
		if localIPs[host] && port == f.port {
			localDevices = append(localDevices, device)
		}
	}
	return localDevices, nil
}

func (f *HeckObjectFactory) ecNurseryData(nodes ring.NodeSlice, hash string, dataFile string) bool {
	wg := sync.WaitGroup{}
	success := true
	fp, err := os.Open(dataFile)
	if err != nil {
		return false
	}

	// TODO: get from metadata
	dataChunks := 6
	parityChunks := 4
	chunkSize := 1 << 20
	contentLength := int64(0)
	if fi, err := fp.Stat(); err != nil {
		return false
	} else {
		contentLength = fi.Size()
	}

	defer fp.Close()
	readers := make([]*io.PipeReader, dataChunks+parityChunks)
	writers := make([]io.Writer, dataChunks+parityChunks)
	for i := 0; i < (dataChunks + parityChunks); i++ {
		rp, wp := io.Pipe()
		defer rp.Close()
		defer wp.Close()
		readers[i] = rp
		writers[i] = wp
	}
	for i, node := range nodes {
		addparts := strings.Split(node.Address(0), "/")
		if len(addparts) != 2 {
			return false
		}
		req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s/ec-frag/%s/%s", addparts[0], addparts[1], hash), readers[i])
		if err != nil {
			return false
		}
		req.Header.Set("FragIndex", strconv.Itoa(i))
		req.Header.Set("HeckPolicy", fmt.Sprintf("%d/%d", 6, 4))
		req.Header.Set("ChunkSize", strconv.Itoa(chunkSize))
		wg.Add(1)
		go func(req *http.Request) {
			defer wg.Done()
			resp, err := f.client.Do(req)
			if err != nil {
				success = false
				return
			}
			resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				success = false
				return
			}
		}(req)
	}
	ECSplit(6, 4, fp, chunkSize, contentLength, writers)
	wg.Wait()
	return success
}

func (f *HeckObjectFactory) ecNurseryDelete(nodes ring.NodeSlice, hash string) bool {
	wg := sync.WaitGroup{}
	success := true
	for _, node := range nodes {
		addparts := strings.Split(node.Address(0), "/")
		if len(addparts) != 2 {
			success = false
			break
		}
		req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/ec-frag/%s/%s", addparts[0], addparts[1], hash), nil)
		if err != nil {
			success = false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := f.client.Do(req)
			if err != nil {
				success = false
				return
			}
			resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				success = false
				return
			}
		}()
	}
	wg.Wait()
	return success
}

func (f *HeckObjectFactory) ecNurseryHandler(writer http.ResponseWriter, request *http.Request) {
	defer f.LogPanics("PANIC REPLICATING DEVICE")
	vars := hummingbird.GetVars(request)

	nurseryPath := filepath.Join(f.driveRoot, vars["device"], "nursery")
	if fi, err := os.Stat(nurseryPath); err != nil || !fi.Mode().IsDir() {
		hummingbird.StandardResponse(writer, 404)
		f.LogError("[replicateDevice] No objects found: %s", nurseryPath)
		return
	}
	nurseryEntries, err := filepath.Glob(filepath.Join(nurseryPath, "????????????????????????????????"))
	if err != nil {
		hummingbird.StandardResponse(writer, 501)
		f.LogError("[replicateDevice] Error getting partition list: %s (%v)", nurseryPath, err)
		return
	}
	// TODO: shuffle nurseryEntries
	for _, entry := range nurseryEntries {
		hash := filepath.Base(entry)
		val, err := strconv.ParseUint(hash[0:8], 16, 64)
		if err != nil {
			continue
		}
		partition := uint32(val >> (64 - f.Ring.PartitionBitCount()))
		nodes := f.Ring.ResponsibleNodes(partition)
		dataFile, _ := objectserver.ObjectFiles(entry)
		if strings.HasSuffix(dataFile, ".ts") {
			f.ecNurseryDelete(nodes, hash)
		} else if strings.HasSuffix(dataFile, ".data") {
			f.ecNurseryData(nodes, hash, dataFile)
		}
	}
}
