package tools

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/troubling/hummingbird/common/ring"
	"go.uber.org/zap"
)

type quarantineRepairman struct {
	aa *AutoAdmin
	// delay between each request; adjusted each pass to try to make passes an hour
	delay time.Duration
}

func newQuarantineRepairman(aa *AutoAdmin) *quarantineRepairman {
	return &quarantineRepairman{aa: aa, delay: time.Second}
}

func (qr *quarantineRepairman) runForever() {
	for {
		qr.runOnce()
	}
}

func (qr *quarantineRepairman) runOnce() {
	start := time.Now()
	logger := qr.aa.logger.With(zap.String("process", "quarantine repairman"))
	logger.Debug("starting pass")
	delays := 0
	for url, ipp := range qr.quarantineDetailURLs() {
		getLogger := logger.With(zap.String("method", "GET"), zap.String("url", url))
		for typ, deviceToEntries := range qr.retrieveTypeToDeviceToEntries(getLogger, url) {
			for device, entries := range deviceToEntries {
				policy := 0
				if typ == "accounts" {
					typ = "account"
				} else if typ == "containers" {
					typ = "container"
				} else if typ == "objects" {
					typ = "object"
				} else if strings.HasPrefix(typ, "objects-") {
					var err error
					policy, err = strconv.Atoi(typ[len("objects-"):])
					if err != nil {
						getLogger.Debug("Weird type in entries", zap.String("type", typ), zap.Error(err))
						continue
					}
					typ = "object"
				} else {
					getLogger.Debug("Weird type in entries", zap.String("type", typ))
					continue
				}
				ringg, _ := getRing("", typ, policy)
				deviceID := -1
				for _, dev := range ringg.AllDevices() {
					if dev.Ip == ipp.ip && dev.Port == ipp.port && dev.Device == device {
						deviceID = dev.Id
						break
					}
				}
				deviceLogger := logger.With(zap.String("type", typ), zap.Int("policy", policy), zap.String("ip", ipp.ip), zap.Int("port", ipp.port), zap.String("device", device), zap.Int("deviceID", deviceID))
				for _, entry := range entries {
					if typ == "object" && entry.NameInURL != "" {
						if !qr.repairObject(deviceLogger, typ, policy, ringg, entry.NameInURL) {
							continue
						}
					} else { // entry.NameOnDevice
						if !qr.queuePartitionReplication(deviceLogger, typ, policy, ringg, deviceID, entry.NameOnDevice) {
							continue
						}
					}
					qr.clearQuarantine(deviceLogger, ipp, device, typ, policy, entry.NameOnDevice)
				}
			}
		}
		delays++
		time.Sleep(qr.delay)
	}
	logger.Debug("pass complete")
	sleepTo := time.Until(start.Add(time.Hour))
	if sleepTo > 0 {
		time.Sleep(sleepTo)
	}
	qr.delay = time.Hour / time.Duration(delays)
}

// quarantineDetailURLs returns a map of urls to ip-port structures based on
// all the servers in all the rings for the hummingbird configuration.
func (qr *quarantineRepairman) quarantineDetailURLs() map[string]*ippInstance {
	urls := map[string]*ippInstance{}
	for _, typ := range []string{"account", "container", "object"} {
		if typ == "object" {
			for _, policy := range qr.aa.policies {
				ringg, _ := getRing("", typ, policy.Index)
				for _, dev := range ringg.AllDevices() {
					urls[fmt.Sprintf("%s://%s:%d/recon/quarantineddetail", dev.Scheme, dev.Ip, dev.Port)] = &ippInstance{scheme: dev.Scheme, ip: dev.Ip, port: dev.Port}
				}
			}
		} else {
			ringg, _ := getRing("", typ, 0)
			for _, dev := range ringg.AllDevices() {
				urls[fmt.Sprintf("%s://%s:%d/recon/quarantineddetail", dev.Scheme, dev.Ip, dev.Port)] = &ippInstance{scheme: dev.Scheme, ip: dev.Ip, port: dev.Port}
			}
		}
	}
	return urls
}

// retrieveTypeToDeviceToEntries performs an HTTP GET request to the url and
// translates the response into a map[string]map[string][]*entryInstance giving
// the types-to-devices-to-entries of quarantined items that server has; any
// errors will be logged and an empty map returned.
func (qr *quarantineRepairman) retrieveTypeToDeviceToEntries(logger *zap.Logger, url string) map[string]map[string][]*entryInstance {
	// type is accounts, containers, objects, objects-1, etc.
	typeToDeviceToEntries := map[string]map[string][]*entryInstance{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logger.Error("http.NewRequest", zap.Error(err))
		return typeToDeviceToEntries
	}
	resp, err := qr.aa.client.Do(req)
	if err != nil {
		logger.Debug("Do", zap.Error(err))
		return typeToDeviceToEntries
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		logger.Debug("Body", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
		return typeToDeviceToEntries
	}
	if resp.StatusCode/100 != 2 {
		logger.Debug("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
		return typeToDeviceToEntries
	}
	if err := json.Unmarshal(body, &typeToDeviceToEntries); err != nil {
		logger.Debug("JSON", zap.String("JSON", string(body)), zap.Error(err))
		return typeToDeviceToEntries
	}
	return typeToDeviceToEntries
}

// repairObject tries to ensure all replicas are in place for the quarantined
// entry and returns true if the entry should be deleted as it has been handled
// as best as is possible.
func (qr *quarantineRepairman) repairObject(logger *zap.Logger, typ string, policy int, ringg ring.Ring, entryNameInURL string) bool {
	logger = logger.With(zap.String("name in URL", entryNameInURL))
	parts := strings.SplitN(entryNameInURL, "/", 4)
	var account, container, object string
	switch len(parts) {
	case 2:
		account = parts[1]
	case 3:
		account = parts[1]
		container = parts[2]
	case 4:
		account = parts[1]
		container = parts[2]
		object = parts[3]
	default:
		logger.Debug("oddball quarantined item")
		return true
	}
	if account == "" || (container == "" && object != "") {
		logger.Debug("oddball quarantined item")
		return true
	}
	switch typ {
	case "account":
		if container != "" || object != "" {
			logger.Debug("skipping item since it doesn't match type 'account'")
			return false
		}
	case "container":
		if container == "" || object != "" {
			logger.Debug("skipping item since it doesn't match type 'container'")
			return false
		}
	case "object":
		if container == "" || object == "" {
			logger.Debug("skipping item since it doesn't match type 'object'")
			return false
		}
	}
	partition := ringg.GetPartition(account, container, object)
	logger = logger.With(zap.Uint64("partition", partition))
	var have, notfound, unsure []*ring.Device
	for _, device := range ringg.GetNodes(partition) {
		url := fmt.Sprintf("%s://%s:%d/%s/%d/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, account)
		if container != "" {
			url += "/" + container
			if object != "" {
				url += "/" + object
			}
		}
		logger = logger.With(zap.String("method", "HEAD"), zap.String("url", url))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			logger.Error("http.NewRequest", zap.Error(err))
			return false
		}
		resp, err := qr.aa.client.Do(req)
		if err != nil {
			logger.Debug("Do", zap.Error(err))
			return false
		}
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			have = append(have, device)
		} else if resp.StatusCode == 404 {
			notfound = append(notfound, device)
		} else {
			logger.Debug("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
			unsure = append(unsure, device)
		}
	}
	if uint64(len(notfound)) == ringg.ReplicaCount() {
		logger.Debug("none of the primary devices had the item so we'll just assume it was deleted after it got quarantined")
		return true
	}
	if len(have) == 0 {
		logger.Debug("couldn't find anyone with the item yet, but not everyone reported in, so just skip for now")
		return false
	}
	fromURL := fmt.Sprintf("%s://%s:%d/%s/%d/%s", have[0].Scheme, have[0].Ip, have[0].Port, have[0].Device, partition, account)
	if container != "" {
		fromURL += "/" + container
		if object != "" {
			fromURL += "/" + object
		}
	}
	logger = logger.With(zap.String("fromURL", fromURL))
	putCopy := func(device *ring.Device) bool {
		fromReq, err := http.NewRequest("GET", fromURL, nil)
		if err != nil {
			logger.Error("http.NewRequest", zap.Error(err))
			return false
		}
		fromResp, err := qr.aa.client.Do(fromReq)
		if err != nil {
			logger.Debug("Do", zap.Error(err))
			return false
		}
		defer fromResp.Body.Close()
		if fromResp.StatusCode/100 != 2 {
			logger.Debug("StatusCode", zap.Int("StatusCode", fromResp.StatusCode), zap.Error(err))
			return false
		}
		toURL := fmt.Sprintf("%s://%s:%d/%s/%d/%s", device.Scheme, device.Ip, device.Port, device.Device, partition, account)
		if container != "" {
			toURL += "/" + container
			if object != "" {
				toURL += "/" + object
			}
		}
		logger = logger.With(zap.String("toURL", toURL))
		toReq, err := http.NewRequest("PUT", toURL, fromResp.Body)
		if err != nil {
			logger.Error("http.NewRequest", zap.Error(err))
			return false
		}
		toReq.Header = fromResp.Header
		toResp, err := qr.aa.client.Do(toReq)
		if err != nil {
			logger.Debug("Do", zap.Error(err))
			return false
		}
		toResp.Body.Close()
		if toResp.StatusCode/100 != 2 {
			logger.Debug("StatusCode", zap.Int("StatusCode", toResp.StatusCode), zap.Error(err))
			return false
		}
		return true
	}
	for _, device := range notfound {
		if !putCopy(device) {
			return false
		}
	}
	for _, device := range unsure {
		if !putCopy(device) {
			return false
		}
	}
	return true
}

// queuePartitionReplication tries to figure out the partition for the
// quarantined entry and queue replication of that partition; returns true if
// the entry should be deleted as it has been handled as best as is possible.
func (qr *quarantineRepairman) queuePartitionReplication(logger *zap.Logger, typ string, policy int, ringg ring.Ring, deviceID int, entryNameOnDevice string) bool {
	logger = logger.With(zap.String("name on device", entryNameOnDevice))
	if typ == "account" || typ == "container" {
		logger.Debug("skipping accounts and containers since they do not have priority replication, for now. There hasn't been a need for it yet because their replication passes have been so quick.")
		return true
	}
	if deviceID < 0 {
		logger.Debug("skipping since deviceID could not be resolved; likely the service we got the report from isn't actually in charge of the device and it's instead another service on the same server using the same device root")
		return false
	}
	if len(entryNameOnDevice) < 8 {
		logger.Debug("oddball quarantined item")
		return true
	}
	hsh, err := strconv.ParseUint(entryNameOnDevice[:8], 16, 64)
	if err != nil {
		logger.Debug("oddball quarantined item")
		return true
	}
	partition := ringg.PartitionForHash(hsh)
	db, err := qr.aa.getDB()
	if err != nil {
		logger.Error("cannot get database", zap.Error(err))
		return false
	}
	tx, err := db.Begin()
	if err != nil {
		logger.Error("db.Begin", zap.Error(err))
		return false
	}
	rows, err := tx.Query(`
        SELECT rtype FROM replication_queue
        WHERE rtype = ?
          AND policy = ?
          AND partition = ?
          AND reason = "quarantine"
          AND to_device = ?
    `, typ, policy, partition, deviceID)
	if err != nil {
		tx.Rollback()
		logger.Error("tx.Query", zap.Error(err))
		return false
	}
	if rows.Next() { // entry already
		rows.Close()
		tx.Rollback()
		return true
	}
	_, err = tx.Exec(`
        INSERT INTO replication_queue
        (rtype, policy, partition, reason, to_device)
        VALUES (?, ?, ?, "quarantine", ?)
    `, typ, policy, partition, deviceID)
	if err != nil {
		tx.Rollback()
		logger.Error("tx.Exec", zap.Error(err))
		return false
	}
	err = tx.Commit()
	if err != nil {
		logger.Error("tx.Commit", zap.Error(err))
		return false
	}
	return true
}

type ippInstance struct {
	scheme string
	ip     string
	port   int
}

type entryInstance struct {
	NameOnDevice string
	NameInURL    string
}

func (qr *quarantineRepairman) clearQuarantine(logger *zap.Logger, ipp *ippInstance, device, typ string, policy int, nameOnDevice string) error {
	reconType := typ + "s"
	if policy != 0 {
		reconType += fmt.Sprintf("-%d", policy)
	}
	url := fmt.Sprintf("%s://%s:%d/", ipp.scheme, ipp.ip, ipp.port) + path.Join("recon", device, "quarantined", reconType, nameOnDevice)
	logger = logger.With(zap.String("method", "DELETE"), zap.String("url", url))
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		logger.Error("http.NewRequest", zap.Error(err))
		return err
	}
	resp, err := qr.aa.client.Do(req)
	if err != nil {
		logger.Debug("Do", zap.Error(err))
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		logger.Debug("StatusCode", zap.Int("StatusCode", resp.StatusCode), zap.Error(err))
		return fmt.Errorf("bad response status code %d", resp.StatusCode)
	}
	logger.Debug("cleared")
	return nil
}