//  Copyright (c) 2015 Rackspace
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

package objectserver

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/mattn/go-sqlite3"
	"github.com/troubling/hummingbird/common"
	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/fs"
)

const (
	schema = `
		CREATE TABLE IF NOT EXISTS object (
			ringhash TEXT PRIMARY KEY,
			part INTEGER,
			deleted INTEGER,
			data_timestamp TEXT,
			metadata TEXT,
			size INTEGER,
			filename TEXT,
			slaboffset INTEGER,
			slabsize INTEGER
		);

		CREATE TABLE IF NOT EXISTS freeblock (
			offset INTEGER PRIMARY KEY,
			size INTEGER
		);

		CREATE TABLE IF NOT EXISTS suffixhashes (
			part INTEGER,
			suffix TEXT,
			hash TEXT,
			PRIMARY KEY (part, suffix)
		);

		CREATE INDEX IF NOT EXISTS ix_freeblock_size ON freeblock (size);

		CREATE INDEX IF NOT EXISTS ix_object_deleted ON object (data_timestamp) WHERE deleted == 1;

		CREATE TRIGGER IF NOT EXISTS tr_object_insert AFTER INSERT ON object

		BEGIN
			INSERT OR REPLACE INTO suffixhashes (part, suffix, hash) VALUES (
				new.part,
				substr(new.ringhash, 30, 3),
				chexor(
					IFNULL(
						(SELECT hash FROM suffixhashes WHERE part=new.part AND suffix=substr(new.ringhash, 30, 3)),
						'00000000000000000000000000000000'
					),
					new.ringhash,
					new.data_timestamp)
				);
		END;

		CREATE TRIGGER IF NOT EXISTS tr_object_delete AFTER DELETE ON object
		BEGIN
			UPDATE suffixhashes SET hash=chexor(hash, old.ringhash, old.data_timestamp)
			WHERE part=old.part AND suffix=substr(old.ringhash, 30, 3);
		END; 
		CREATE TRIGGER IF NOT EXISTS tr_object_update AFTER UPDATE ON object
			BEGIN
			UPDATE suffixhashes SET hash=chexor(chexor(hash, old.ringhash, old.data_timestamp), new.ringhash, new.data_timestamp)
			WHERE part=old.part AND suffix=substr(old.ringhash, 30, 3);
		END;`

	pragmaScript = `
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -4096;
		PRAGMA temp_store = MEMORY;
		PRAGMA journal_mode = WAL;
		PRAGMA busy_timeout = 25000;`
)

type SlabObject struct {
	factory  *SlabEngine
	file     *os.File
	slabm    *sync.Mutex
	afw      fs.AtomicFileWriter
	size     int64
	part     int64
	sizeLeft int64
	metadata map[string]string
	pos      int64
	data     []byte
	tempDir  string
	destDir  string
	exists   bool
	db       *sql.DB
	ringhash string
}

func (o *SlabObject) Write(b []byte) (int, error) {
	var err error
	if o.afw != nil {
		return o.afw.Write(b)
	}
	if int64(len(b))+int64(len(o.data)) > o.factory.slabCutoffSize {
		o.afw, err = fs.NewAtomicFileWriter(o.tempDir, o.destDir)
		if err != nil {
			return 0, err
		}
		_, err := o.afw.Write(o.data)
		o.data = nil
		if err != nil {
			return 0, err
		}
		return o.afw.Write(b)
	}
	o.data = append(o.data, b...)
	return len(b), nil
}

func (o *SlabObject) Read(b []byte) (read int, err error) {
	if o.sizeLeft == 0 {
		return 0, io.EOF
	} else if o.sizeLeft < int64(len(b)) {
		read, err = syscall.Pread(int(o.file.Fd()), b[:int(o.sizeLeft)], o.pos)
	} else {
		read, err = syscall.Pread(int(o.file.Fd()), b, o.pos)
	}
	o.pos += int64(read)
	o.sizeLeft -= int64(read)
	return
}

func (o *SlabObject) Metadata() map[string]string {
	return o.metadata
}

func (o *SlabObject) ContentLength() int64 {
	return o.size
}

func (o *SlabObject) Quarantine() error {
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var filename string
	var slaboffset, slabsize int64
	if err := tx.QueryRow("SELECT filename, slaboffset, slabsize FROM object WHERE ringhash = ?", o.ringhash).Scan(&filename, &slaboffset, &slabsize); err != nil {
		return err
	}
	if slabsize != 0 {
		if _, err := tx.Exec("INSERT INTO freeblock (size, offset) VALUES (?, ?)", slabsize, slaboffset); err != nil {
			return err
		}
	} else if filename != "" {
		go os.Remove(filename)
	}
	if _, err := tx.Exec("DELETE FROM object WHERE ringhash = ?", o.ringhash); err != nil {
		return err
	}
	return tx.Commit()
}

func (o *SlabObject) Exists() bool {
	return o.exists
}

func (o *SlabObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if len(dsts) == 1 {
		return io.Copy(dsts[0], o)
	} else {
		return common.Copy(o, dsts...)
	}
}

func (o *SlabObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	o.pos = start
	return common.CopyN(o, end-start, w)
}

func (o *SlabObject) Repr() string {
	return "SlabObject(" + o.ringhash + ")"
}

func (o *SlabObject) SetData(size int64) (io.Writer, error) {
	return o, nil
}

func pwriteAll(fd int, data []byte, offset int64) error {
	for len(data) != 0 {
		n, err := syscall.Pwrite(fd, data, offset)
		if err != nil {
			return err
		}
		offset += int64(n)
		data = data[n:]
	}
	return nil
}

func (o *SlabObject) getSlabSpace(size int64) (int64, int64, error) {
	var offset, slabsize int64

	tx, err := o.db.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	allocSize := int64(math.Pow(2, math.Ceil(math.Log2(float64(size)))))
	if (size > 1024) && (size%1024 != 0) {
		allocSize = size + (1024 - (5678 % 1024))
	}

	min := size
	max := size + o.factory.allowedSlabWaste
	if size <= 1024 {
		max = allocSize
	}
	if err := tx.QueryRow("SELECT offset, size FROM freeblock WHERE size BETWEEN ? AND ?", min, max).Scan(&offset, &slabsize); err == nil {
		if _, err := tx.Exec("DELETE FROM freeblock WHERE offset = ?", offset); err == nil {
			return offset, slabsize, tx.Commit()
		}
	}
	tx.Rollback()

	o.slabm.Lock()
	defer o.slabm.Unlock()
	if err := syscall.Flock(int(o.file.Fd()), syscall.LOCK_EX); err != nil {
		return 0, 0, err
	}
	defer syscall.Flock(int(o.file.Fd()), syscall.LOCK_UN)
	stat, err := o.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	offset = stat.Size()
	if err := o.file.Truncate(offset + allocSize); err != nil {
		return 0, 0, err
	}
	return offset, allocSize, nil
}

func (o *SlabObject) Commit(metadata map[string]string) error {
	sermeta, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	timestamp, ok := metadata["X-Timestamp"]
	if !ok {
		return errors.New("No timestamp in metadata")
	}
	var slabOffset, slabSize, size int64
	var filename string
	if o.afw != nil {
		defer o.afw.Abandon()
		size, err = strconv.ParseInt(metadata["Content-Length"], 10, 64)
		if err != nil {
			return err
		}
		filename = filepath.Join(o.destDir, o.ringhash+"-"+strconv.FormatInt(rand.Int63(), 10))
		if err := o.afw.Save(filename); err != nil {
			return err
		}
	} else if len(o.data) > 0 {
		size = int64(len(o.data))
		slabOffset, slabSize, err = o.getSlabSpace(size)
		if err != nil {
			return err
		}
		if err := pwriteAll(int(o.file.Fd()), o.data, slabOffset); err != nil {
			return err
		}
		o.file.Sync()
	}

	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var eso, ess int64
	var efn, edt string
	if err := tx.QueryRow("SELECT data_timestamp, filename, slaboffset, slabsize FROM object WHERE ringhash = ? LIMIT 1", o.ringhash).Scan(
		&edt, &efn, &eso, &ess); err == nil {
		if edt > timestamp {
			return nil
		}
		if _, err = tx.Exec("UPDATE object SET deleted=0, data_timestamp=?, metadata=?, size=?, filename=?, slaboffset=?, slabsize=? WHERE ringhash=?",
			timestamp, string(sermeta), size, filename, slabOffset, slabSize, o.ringhash); err != nil {
			return err
		}
		if efn == "" && ess != 0 {
			if _, err := tx.Exec("INSERT INTO freeblock (size, offset) VALUES (?, ?)", ess, eso); err != nil {
				return err
			}
		} else if efn != "" {
			go os.Remove(efn)
		}
	} else {
		_, err := tx.Exec("INSERT INTO object (ringhash, part, deleted, data_timestamp, metadata, size, filename, slaboffset, slabsize) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			o.ringhash, o.part, 0, timestamp, string(sermeta), size, filename, slabOffset, slabSize)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (o *SlabObject) Delete(metadata map[string]string) error {
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var filename, dataTimestamp string
	var slaboffset, slabsize int64
	if err := tx.QueryRow("SELECT data_timestamp, filename, slaboffset, slabsize FROM object WHERE ringhash = ? LIMIT 1", o.ringhash).Scan(
		&dataTimestamp, &filename, &slaboffset, &slabsize); err == nil {
		if dataTimestamp > metadata["X-Timestamp"] {
			return nil
		}
		if _, err = tx.Exec("UPDATE object SET deleted=1, data_timestamp=?, metadata='', size=0, filename='', slaboffset=0, slabsize=0 WHERE ringhash=?",
			metadata["X-Timestamp"], o.ringhash); err != nil {
			return err
		}
		if filename == "" && slabsize != 0 {
			if _, err := tx.Exec("INSERT INTO freeblock (size, offset) VALUES (?, ?)", slabsize, slaboffset); err != nil {
				return err
			}
		} else if filename != "" {
			go os.Remove(filename)
		}
	} else {
		_, err = o.db.Exec("INSERT INTO object (ringhash, part, deleted, data_timestamp, metadata, size, filename, slaboffset, slabsize) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			o.ringhash, o.part, 1, metadata["X-Timestamp"], "", 0, "", 0, 0)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (o *SlabObject) Close() error {
	if o.afw != nil {
		o.afw.Abandon()
		o.afw = nil
	}
	return nil
}

type SlabEngine struct {
	wholefileDirCount int64
	allowedSlabWaste  int64
	slabCutoffSize    int64
	deviceRoot        string
	hashPathPrefix    string
	hashPathSuffix    string
	reserve           int64
	reclaimAge        int64
	policy            int
	lock              sync.Mutex
	db                map[string]*sql.DB
	slab              map[string]*os.File
	slabm             map[string]*sync.Mutex
}

func (f *SlabEngine) New(vars map[string]string, needData bool, wg *sync.WaitGroup) (Object, error) {
	tempDir := filepath.Join(f.deviceRoot, vars["device"], "tmp")
	if err := os.MkdirAll(tempDir, 0766); err != nil {
		return nil, err
	}
	destDir := filepath.Join(f.deviceRoot, vars["device"], PolicyDir(f.policy), strconv.FormatInt(rand.Int63()%f.wholefileDirCount, 10))
	if err := os.MkdirAll(destDir, 0766); err != nil {
		return nil, err
	}
	ringhash := filepath.Base(ObjHashDir(vars, f.deviceRoot, f.hashPathPrefix, f.hashPathSuffix, f.policy))
	db, err := f.getDb(vars["device"])
	if err != nil {
		return nil, err
	}
	part, err := strconv.ParseInt(vars["partition"], 10, 64)
	if err != nil {
		return nil, err
	}
	var dataTimestamp, filename, metastr string
	var deleted, size, slaboffset, slabsize int64
	err = db.QueryRow("SELECT deleted, data_timestamp, metadata, size, filename, slaboffset, slabsize FROM object WHERE ringhash = ?",
		ringhash).Scan(&deleted, &dataTimestamp, &metastr, &size, &filename, &slaboffset, &slabsize)
	var file *os.File
	var slabm *sync.Mutex
	metadata := map[string]string{}
	if metastr != "" {
		if err := json.Unmarshal([]byte(metastr), &metadata); err != nil {
			return nil, err
		}
	}
	if needData && filename != "" {
		if file, err = os.Open(filename); err != nil {
			return nil, err
		}
		slaboffset = 0
	} else if file, slabm, err = f.getSlab(vars["device"]); err != nil {
		return nil, err
	}
	return &SlabObject{
		factory:  f,
		file:     file,
		afw:      nil,
		data:     nil,
		exists:   (dataTimestamp != "" && deleted == 0),
		size:     size,
		sizeLeft: size,
		pos:      slaboffset,
		metadata: metadata,
		tempDir:  tempDir,
		destDir:  destDir,
		db:       db,
		ringhash: ringhash,
		part:     part,
		slabm:    slabm,
	}, nil
}

func (f *SlabEngine) RegisterHandlers(addRoute func(method, path string, handler http.HandlerFunc)) {
}

func (f *SlabEngine) getDb(device string) (*sql.DB, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if db, ok := f.db[device]; ok {
		return db, nil
	}
	dbpath := filepath.Join(f.deviceRoot, device, PolicyDir(f.policy), "objects.db")
	db, err := sql.Open("slab_sqlite", "file:"+dbpath+"?psow=1&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		return nil, err
	}
	defer tx.Rollback()
	_, err = tx.Exec(schema)
	if err != nil {
		db.Close()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		return nil, err
	}
	f.db[device] = db
	return db, nil
}

func (f *SlabEngine) getSlab(device string) (*os.File, *sync.Mutex, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if slab, ok := f.slab[device]; ok {
		return slab, f.slabm[device], nil
	}
	slabFile := filepath.Join(f.deviceRoot, device, PolicyDir(f.policy), "slab")
	slab, err := os.OpenFile(slabFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}
	f.slab[device] = slab
	f.slabm[device] = new(sync.Mutex)
	return slab, f.slabm[device], nil
}

func SlabEngineConstructor(config conf.Config, policy *conf.Policy, flags *flag.FlagSet) (ObjectEngine, error) {
	deviceRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, errors.New("Unable to load hashpath prefix and suffix")
	}
	reclaimAge := int64(config.GetInt("app:object-server", "reclaim_age", int64(common.ONE_WEEK)))
	slabCutoffSize, err := strconv.ParseInt(policy.Config["slab_cutoff"], 10, 64)
	if err != nil {
		slabCutoffSize = 1024 * 1024 * 2
	}
	allowedSlabWaste, err := strconv.ParseInt(policy.Config["allowed_waste"], 10, 64)
	if err != nil {
		allowedSlabWaste = 256
	}
	wholefileDirCount, err := strconv.ParseInt(policy.Config["wholefile_dirs"], 10, 64)
	if err != nil {
		wholefileDirCount = 32
	}
	return &SlabEngine{
		wholefileDirCount: wholefileDirCount,
		allowedSlabWaste:  allowedSlabWaste,
		slabCutoffSize:    slabCutoffSize,
		db:                make(map[string]*sql.DB),
		slab:              make(map[string]*os.File),
		slabm:             make(map[string]*sync.Mutex),
		deviceRoot:        deviceRoot,
		hashPathPrefix:    hashPathPrefix,
		hashPathSuffix:    hashPathSuffix,
		reserve:           reserve,
		reclaimAge:        reclaimAge,
		policy:            policy.Index}, nil
}

func chexor(old, ringhash, timestamp string) string {
	oldDigest, err := hex.DecodeString(old)
	if err != nil {
		panic(fmt.Sprintf("Error decoding hex: %v", err))
	}
	h := md5.New()
	if _, err := io.WriteString(h, ringhash+"-"+timestamp); err != nil {
		panic("THIS SHOULD NEVER HAPPEN")
	}
	digest := h.Sum(nil)
	for i := range digest {
		digest[i] ^= oldDigest[i]
	}
	return hex.EncodeToString(digest)
}

func init() {
	sql.Register("slab_sqlite",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				if err := conn.RegisterFunc("chexor", chexor, true); err != nil {
					return err
				}
				if _, err := conn.Exec(pragmaScript, nil); err != nil {
					return err
				}
				return nil
			},
		},
	)
	RegisterObjectEngine("slab", SlabEngineConstructor)
}

// make sure these things satisfy interfaces at compile time
var _ ObjectEngineConstructor = SlabEngineConstructor
var _ Object = &SlabObject{}
var _ ObjectEngine = &SlabEngine{}
