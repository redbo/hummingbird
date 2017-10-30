package internal

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
)

func errnil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func md5hash(data string) string {
	b := md5.Sum([]byte(data))
	return hex.EncodeToString(b[:])
}

func newTestIndexDB(t *testing.T, pth string) *IndexDB {
	t.Helper()
	ot, err := NewIndexDB(pth, pth, pth, 2, 1, 1, zap.L())
	errnil(t, err)
	return ot
}

func TestNewIndexDB_notExistsAndAlreadyExists(t *testing.T) {
	pth := "testdata/tmp/TestNewIndexDB_notExistsAndAlreadyExists"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	ot.Close()
	ot = newTestIndexDB(t, pth)
}

func TestIndexDB_Commit(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Commit"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Initial commit.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil))
	pth, err = ot.wholeObjectPath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err := os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Same commit should return (nil, nil).
	f, err = ot.TempFile(hsh, 0, timestamp, int64(len(body)))
	if f != nil || err != nil {
		t.Fatal(f, err)
	}
	// So we'll fake the timestamp on the TempFile call, but try to do the same
	// as the stored timestamp on the commit again, which should not fail (but
	// won't really do anything behind the scenes).
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil))
	pth, err = ot.wholeObjectPath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Attempting an older commit should return (nil, nil).
	f, err = ot.TempFile(hsh, 0, timestamp-1, int64(len(body)))
	if f != nil || err != nil {
		t.Fatal(f, err)
	}
	// Doing an older commit should be discarded. We're going to fake like
	// we're doing a newer commit to get the temp file, but then actually try
	// to do an old commit.
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp-1, false, "", nil))
	pth, err = ot.wholeObjectPath(hsh, 0, timestamp-1)
	fi, err = os.Stat(pth)
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// Original commit should still be there.
	pth, err = ot.wholeObjectPath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Doing a newer commit should discard the original.
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp+1, false, "", nil))
	pth, err = ot.wholeObjectPath(hsh, 0, timestamp+1)
	errnil(t, err)
	fi, err = os.Stat(pth)
	errnil(t, err)
	if fi.Size() != int64(len(body)) {
		t.Fatal(fi.Size(), len(body))
	}
	// Original commit should be gone.
	pth, err = ot.wholeObjectPath(hsh, 0, timestamp)
	errnil(t, err)
	fi, err = os.Stat(pth)
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestIndexDB_Lookup(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Lookup"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil))
	// Do the lookup.
	lookedupTimestamp, deletion, metahash, metadata, path, err := ot.Lookup(hsh, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if deletion {
		t.Fatal(deletion)
	}
	if metahash != "" || metadata != nil {
		t.Fatalf("%#v %#v\n", metahash, metadata)
	}
	if path == "" {
		t.Fatal(path)
	}
	// Check the file.
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(body) {
		t.Fatal(len(b), len(body))
	}
	if string(b) != body {
		t.Fatal(string(b), body)
	}
}

func TestIndexDB_Lookup_withOverwrite(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Lookup_withOverwrite"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil))
	// Commit newer file.
	timestamp = time.Now().UnixNano()
	body = "just testing newer"
	f, err = ot.TempFile(hsh, 0, timestamp, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil))
	// Do the lookup.
	lookedupTimestamp, deletion, metahash, metadata, path, err := ot.Lookup(hsh, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if deletion {
		t.Fatal(deletion)
	}
	if metahash != "" || metadata != nil {
		t.Fatalf("%#v %#v\n", metahash, metadata)
	}
	if path == "" {
		t.Fatal(path)
	}
	// Check the file.
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(body) {
		t.Fatal(len(b), len(body))
	}
	if string(b) != body {
		t.Fatal(string(b), body)
	}
}

func TestIndexDB_Lookup_withUnderwrite(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_Lookup_withUnderwrite"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	hsh := md5hash("object1")
	timestamp := time.Now().UnixNano()
	body := "just testing"
	// Commit file.
	f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)))
	errnil(t, err)
	f.Write([]byte(body))
	errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil))
	// Commit older file (should be discarded).
	timestampOlder := timestamp - 1
	bodyOlder := "just testing older"
	// Fake newer commit, but we'll really commit with timestampOlder.
	f, err = ot.TempFile(hsh, 0, timestamp+1, int64(len(bodyOlder)))
	errnil(t, err)
	f.Write([]byte(bodyOlder))
	errnil(t, ot.Commit(f, hsh, 0, timestampOlder, false, "", nil))
	// Do the lookup.
	lookedupTimestamp, deletion, metahash, metadata, path, err := ot.Lookup(hsh, 0)
	if err != nil {
		t.Fatal(err)
	}
	if lookedupTimestamp != timestamp {
		t.Fatal(lookedupTimestamp, timestamp)
	}
	if deletion {
		t.Fatal(deletion)
	}
	if metahash != "" || metadata != nil {
		t.Fatalf("%#v %#v\n", metahash, metadata)
	}
	if path == "" {
		t.Fatal(path)
	}
	// Check the file.
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(body) {
		t.Fatal(len(b), len(body))
	}
	if string(b) != body {
		t.Fatal(string(b), body)
	}
}

func TestIndexDB_List(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_List"
	defer os.RemoveAll(pth)
	ot := newTestIndexDB(t, pth)
	defer ot.Close()
	matchHashes0_0 := map[string]struct{}{}
	matchHashes0_1 := map[string]struct{}{}
	matchHashes1_1 := map[string]struct{}{}
	// Create a bunch of objects.
	for i := 0; i < 32; i++ {
		hsh := md5hash(fmt.Sprintf("object%d", i))
		hshb, err := hex.DecodeString(hsh)
		if err != nil {
			t.Fatal(err)
		}
		if hshb[0]>>(8-ot.RingPartPower) == 0 {
			matchHashes0_0[hsh] = struct{}{}
		}
		if hshb[0]>>(8-ot.RingPartPower) == 0 {
			matchHashes0_1[hsh] = struct{}{}
		}
		if hshb[0]>>(8-ot.RingPartPower) == 1 {
			matchHashes1_1[hsh] = struct{}{}
		}
		timestamp := time.Now().UnixNano()
		body := "just testing"
		f, err := ot.TempFile(hsh, 0, timestamp, int64(len(body)))
		errnil(t, err)
		f.Write([]byte(body))
		errnil(t, ot.Commit(f, hsh, 0, timestamp, false, "", nil))
	}
	listing, err := ot.List(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range listing {
		if _, ok := matchHashes0_0[item.Hash]; !ok {
			t.Error(item.Hash)
		}
		delete(matchHashes0_0, item.Hash)
	}
	if len(matchHashes0_0) != 0 {
		t.Error(matchHashes0_0)
	}
	listing, err = ot.List(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range listing {
		if _, ok := matchHashes0_1[item.Hash]; !ok {
			t.Error(item.Hash)
		}
		delete(matchHashes0_1, item.Hash)
	}
	if len(matchHashes0_1) != 0 {
		t.Error(matchHashes0_1)
	}
	listing, err = ot.List(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range listing {
		if _, ok := matchHashes1_1[item.Hash]; !ok {
			t.Error(item.Hash)
		}
		delete(matchHashes1_1, item.Hash)
	}
	if len(matchHashes1_1) != 0 {
		t.Error(matchHashes1_1)
	}
}

func TestIndexDB_ringPartRange(t *testing.T) {
	pth := "testdata/tmp/TestIndexDB_partitionRange"
	defer os.RemoveAll(pth)
	ot, err := NewIndexDB(pth, pth, pth, 4, 1, 1, zap.L())
	errnil(t, err)
	defer ot.Close()
	startHash, stopHash := ot.ringPartRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "0fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.ringPartRange(7)
	if startHash != "70000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.ringPartRange(15)
	if startHash != "f0000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	ot, err = NewIndexDB(pth, pth, pth, 8, 1, 1, zap.L())
	errnil(t, err)
	defer ot.Close()
	startHash, stopHash = ot.ringPartRange(0)
	if startHash != "00000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "00ffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.ringPartRange(127)
	if startHash != "7f000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "7fffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
	startHash, stopHash = ot.ringPartRange(255)
	if startHash != "ff000000000000000000000000000000" {
		t.Fatal(startHash)
	}
	if stopHash != "ffffffffffffffffffffffffffffffff" {
		t.Fatal(stopHash)
	}
}