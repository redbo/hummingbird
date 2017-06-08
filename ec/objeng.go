//  Copyright (c) 2016 Rackspace

package ec

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gholt/ring"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/objectserver"
)

type HeckObjectFactory struct {
	driveRoot      string
	reserve        int64
	checkMounts    bool
	port           int
	hashPathPrefix string
	hashPathSuffix string
	Ring           ring.Ring
	client         *http.Client
	gossip         *GossipState
	logger         hummingbird.SysLogLike
}

var _ objectserver.ObjectEngine = &HeckObjectFactory{}

func (f *HeckObjectFactory) LogError(format string, args ...interface{}) {
	f.logger.Err(fmt.Sprintf(format, args...))
}

func (f *HeckObjectFactory) LogInfo(format string, args ...interface{}) {
	f.logger.Info(fmt.Sprintf(format, args...))
}

func (f *HeckObjectFactory) LogDebug(format string, args ...interface{}) {
	f.logger.Debug(fmt.Sprintf(format, args...))
}

func (f *HeckObjectFactory) LogPanics(m string) {
	if er := recover(); er != nil {
		f.LogError("%s: %s: %s", m, er, debug.Stack())
	}
}

func (f *HeckObjectFactory) New(vars map[string]string, needData bool) (objectserver.Object, error) {
	var err error
	hash := ObjHash(vars, f.driveRoot, f.hashPathPrefix, f.hashPathSuffix)
	eo := &HeckObject{
		HeckObjectFactory: f,
		hash:              hash,
		nurseryPath:       filepath.Join(f.driveRoot, vars["device"], "ec", "nursery", hash),
		ofhPath:           filepath.Join(f.driveRoot, vars["device"], "ec", "ofh", hash[0:2], hash[2:4], hash[4:6], hash),
		location:          notFound,
	}
	dataFile, metaFile := objectserver.ObjectFiles(eo.nurseryPath)
	if strings.HasPrefix(dataFile, ".data") {
		if eo.metadata, err = objectserver.ObjectMetadata(dataFile, metaFile); err != nil {
			return nil, err
		}
		eo.location = inNursery
		return eo, nil
	}
	if hummingbird.Exists(eo.ofhPath) {
		eo.metadata, err = objectserver.ObjectMetadata(eo.ofhPath, "")
		if err != nil {
			return nil, err
		}
		eo.location = inOFH
	}
	return eo, nil
}

func (f *HeckObjectFactory) ecChunkGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	fileName := filepath.Join(f.driveRoot, vars["device"], "ec", "ofh", vars["hash"][0:2], vars["hash"][2:4], vars["hash"][4:6], vars["hash"])
	if !hummingbird.Exists(fileName) {
		hummingbird.StandardResponse(writer, http.StatusNotFound)
		return
	}
	fl, err := os.Open(fileName)
	if err != nil {
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer fl.Close()
	metadata, err := objectserver.OpenObjectMetadata(fl.Fd(), "")
	if err != nil {
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	for key, value := range metadata {
		if key != "name" {
			writer.Header().Set(key, value)
		}
	}
	writer.WriteHeader(http.StatusOK)
	io.Copy(writer, fl)
}

func (f *HeckObjectFactory) ecChunkPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	fileName := filepath.Join(f.driveRoot, vars["device"], "ec", "ofh", vars["hash"][0:2], vars["hash"][2:4], vars["hash"][4:6], vars["hash"])
	fl, err := os.OpenFile(fileName, os.O_WRONLY, 0666)
	if err != nil {
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer fl.Close()
	metadata := make(map[string]string)
	for key := range request.Header {
		if strings.HasPrefix(key, "Ec-") {
			metadata[strings.ToLower(key[3:])] = request.Header.Get(key)
		}
	}
	objectserver.WriteMetadata(fl.Fd(), metadata)
	fl.Sync()
	io.Copy(fl, request.Body)
	hummingbird.StandardResponse(writer, http.StatusCreated)
}

func (f *HeckObjectFactory) ecChunkDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	fileName := filepath.Join(f.driveRoot, vars["device"], "ec", "ofh", vars["hash"][0:2], vars["hash"][2:4], vars["hash"][4:6], vars["hash"])
	err := os.Remove(fileName)
	if err == nil || os.IsNotExist(err) {
		hummingbird.StandardResponse(writer, http.StatusCreated)
	}
	hummingbird.StandardResponse(writer, http.StatusInternalServerError)
}

func (f *HeckObjectFactory) gossipHandler(writer http.ResponseWriter, request *http.Request) {
	var state *GossipState
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		hummingbird.StandardResponse(writer, http.StatusBadRequest)
		return
	}
	if json.Unmarshal(body, &state) == nil {
		f.gossip.MergeState(state)
		hummingbird.StandardResponse(writer, http.StatusOK)
		return
	}
	hummingbird.StandardResponse(writer, http.StatusBadRequest)
}

func (f *HeckObjectFactory) doActions(stop chan struct{}) {
	actionList := f.gossip.ActionList()
	nodes := f.Ring.Nodes()
	hasActions := make(map[uint64]bool)
	actionQueues := make(map[uint64]chan *Action)
	for _, a := range actionList {
		hasActions[a.Src] = true
		hasActions[a.Dst] = true
	}
	for _, node := range nodes {
		if !hasActions[node.ID()] {
			continue
		}
		c := make(chan *Action)
		go func(node ring.Node, c chan *Action) {
			// host, port, device, err := ParseAddress(node.Address(0))
			for action := range c {
				// TODO: do action
				f.gossip.MarkDone(action.Name)
			}
		}(node, c)
		actionQueues[node.ID()] = c
	}
	for {
		select {
		case <-stop:
			for _, c := range actionQueues {
				close(c)
			}
			return
		default:
			action := actionList[len(actionList)-1]
			actionList = actionList[:len(actionList)-1]
			/* TODO: this better */
			go func() {
				actionQueues[action.Src] <- action
			}()
		}
	}
}

func (f *HeckObjectFactory) RegisterHandlers(addRoute func(method, path string, handler http.HandlerFunc)) {
	addRoute("GET", "/ec-frag/:device/:hash", f.ecChunkGetHandler)
	addRoute("PUT", "/ec-frag/:device/:hash", f.ecChunkPutHandler)
	addRoute("DELETE", "/ec-frag/:device/:hash", f.ecChunkDeleteHandler)
	addRoute("POST", "/ec-nursery/:device", f.ecNurseryHandler)
	addRoute("POST", "/ec-gossip", f.gossipHandler)
}

func (f *HeckObjectFactory) Background() {
	var stopDoingActions chan struct{} = nil
	startElection := AlignedTick(electionFrequency, 0)
	endElection := AlignedTick(electionFrequency, electionDuration)
	gossipTicker := AlignedTick(gossipFrequency, 0)
	electionGossipTicker := AlignedTick(electionGossipFrequency, 0)
	for {
		select {
		case <-startElection:
			if stopDoingActions != nil {
				stopDoingActions <- struct{}{}
				close(stopDoingActions)
				stopDoingActions = nil
			}
			f.gossip.StartElection()
		case <-endElection:
			if f.gossip.IsLeader() {
				stopDoingActions = make(chan struct{}, 1)
				go f.doActions(stopDoingActions)
			}
		case <-electionGossipTicker:
			f.gossip.Gossip(f.Ring)
		case <-gossipTicker:
			f.gossip.Gossip(f.Ring)
		}
	}
}

func HeckEngineConstructor(config hummingbird.IniFile, flags *flag.FlagSet) (objectserver.ObjectEngine, error) {
	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	rf, err := os.Open("/etc/swift/ecring.gz")
	if err != nil {
		return nil, errors.New("Unable to open ring")
	}
	defer rf.Close()
	ring, err := ring.LoadRing(rf)
	f := &HeckObjectFactory{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		Ring:           ring,
		gossip:         NewGossipState(),
		port:           int(config.GetInt("object-replicator", "bind_port", 6000)),
		logger:         hummingbird.SetupLogger(config.GetDefault("object-replicator", "log_facility", "LOG_LOCAL0"), "object-ecer", ""),
		checkMounts:    config.GetBool("object-replicator", "mount_check", true),
		client: &http.Client{
			Transport: &http.Transport{Dial: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 10 * time.Second}).Dial},
			Timeout:   time.Minute * 60,
		},
	}
	go f.Background()
	return f, nil
}

func init() {
	objectserver.RegisterObjectEngine("heck", HeckEngineConstructor)
}
