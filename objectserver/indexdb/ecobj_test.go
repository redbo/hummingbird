//  Copyright (c) 2018 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package indexdb

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/troubling/hummingbird/common/ring"
	"github.com/troubling/hummingbird/common/test"
)

func TestNurseryReplicate(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:      http.DefaultClient,
		dataFrags:   3,
		parityFrags: 2,
		chunkSize:   100,
		metadata: map[string]string{
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}
	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockGetJobNodes: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
		},
		MockGetJobNodesHandoff: false,
	}
	require.Nil(t, to.nurseryReplicate(rng, 1, node))
	require.Equal(t, 2, calls)
}

func TestNurseryReplicateWithFailure(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	used := make(map[string]bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drive := r.URL.Path[9:12]
		if drive == "sdb" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		used[drive] = true
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:      http.DefaultClient,
		dataFrags:   3,
		parityFrags: 2,
		chunkSize:   100,
		metadata: map[string]string{
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}
	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockGetJobNodes: []*ring.Device{
			{Id: 1, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdb"},
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
		},
		MockGetJobNodesHandoff: false,
	}
	require.Nil(t, to.nurseryReplicate(rng, 1, node))
	require.False(t, used["sdb"])
	require.True(t, used["sdc"])
	require.True(t, used["sdd"])
}

type CustomFakeRing struct {
	test.FakeRing
}

func (r *CustomFakeRing) GetNodes(partition uint64) (response []*ring.Device) {
	return r.MockDevices
}

func TestStabilize(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	used := make(map[string]bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drive := r.URL.Path[9:12]
		used[drive] = true
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:      http.DefaultClient,
		dataFrags:   3,
		parityFrags: 2,
		chunkSize:   100,
		metadata: map[string]string{
			"name":           "/a/c/o",
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}
	rng := &CustomFakeRing{
		FakeRing: test.FakeRing{
			MockDevices: []*ring.Device{
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sda"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdb"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
				{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sde"},
			},
		},
	}
	require.Nil(t, to.Stabilize(rng, nil, 0))
	require.True(t, used["sda"])
	require.True(t, used["sdb"])
	require.True(t, used["sdc"])
	require.True(t, used["sdd"])
	require.True(t, used["sde"])
}

func TestDontStabilizeWithFailure(t *testing.T) {
	fp, err := ioutil.TempFile("", "")
	fp.Write([]byte("TESTING"))
	require.Nil(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drive := r.URL.Path[9:12]
		if drive == "sdb" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.Copy(ioutil.Discard, r.Body)
	}))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	require.Nil(t, err)
	port, err := strconv.Atoi(u.Port())
	require.Nil(t, err)

	to := &ecObject{
		IndexDBItem: IndexDBItem{
			Hash:     "00000011111122222233333344444455",
			Deletion: false,
			Path:     fp.Name(),
		},
		client:      http.DefaultClient,
		dataFrags:   2,
		parityFrags: 1,
		chunkSize:   100,
		metadata: map[string]string{
			"name":           "/a/c/o",
			"Content-Length": "7",
		},
		nurseryReplicas: 3,
	}

	node := &ring.Device{Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port - 1, Device: "sda"}
	rng := &test.FakeRing{
		MockDevices: []*ring.Device{
			{Id: 2, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdb"},
			{Id: 3, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdc"},
			{Id: 4, Scheme: u.Scheme, ReplicationIp: u.Hostname(), ReplicationPort: port, Device: "sdd"},
		},
	}
	err = to.Stabilize(rng, node, 0)
	require.NotNil(t, err)
	require.Equal(t, "Failed to stabilize object", err.Error())
}

func TestParseECScheme(t *testing.T) {
	algo, dataFrags, parityFrags, chunkSize, err := parseECScheme("reedsolomon/1/2/16")
	require.Nil(t, err)
	require.Equal(t, "reedsolomon", algo)
	require.Equal(t, 1, dataFrags)
	require.Equal(t, 2, parityFrags)
	require.Equal(t, 16, chunkSize)

	algo, dataFrags, parityFrags, chunkSize, err = parseECScheme("1/2/16")
	require.NotNil(t, err)

	algo, dataFrags, parityFrags, chunkSize, err = parseECScheme("reedsolomon/1/2/X")
	require.NotNil(t, err)
}