/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package logstats

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	MAX_NUM_FILES = 99
)

var DEBUG int = 0

// SyncWriteCloser is the writer contract for logstats file handlers.
type SyncWriteCloser interface {
	io.WriteCloser
	Sync() error
}

type FileHandler interface {
	// Open opens the active log file.
	Open(fileName string) (SyncWriteCloser, int, error)

	// Rotate shifts rotated files, moves the active file, opens a new active file.
	// logstats calls Close() on the current writer before calling Rotate.
	Rotate(fileName string, numFiles int) (SyncWriteCloser, int, error)

	// DisableCompression turns off gzip compression for future rotations.
	DisableCompression()
}

type defaultFileHandler struct {
	compress bool
}

func (h *defaultFileHandler) Open(fileName string) (SyncWriteCloser, int, error) {
	f, sz, err := openLogFile(fileName)
	if err != nil {
		return nil, 0, err
	}
	return f, sz, nil
}

func (h *defaultFileHandler) Rotate(fileName string, numFiles int) (SyncWriteCloser, int, error) {
	f, sz, err := rotate(fileName, numFiles, h.compress)
	if err != nil {
		return nil, 0, err
	}
	return f, sz, nil
}

func (h *defaultFileHandler) DisableCompression() {
	h.compress = false
}

func newDefaultFileHandler() FileHandler {
	return &defaultFileHandler{compress: true}
}

// LogStats interface
type LogStats interface {
	// Write stats to the file.
	Write(statType string, statMap map[string]interface{}) error

	// Set flag for durability - when set to true, each call to Write will
	// also call Sync() on the underlying writer if supported.
	SetDurable(durable bool)

	// Closes the log file if open.
	Close()

	// ForceRotate closes the current log file and opens a fresh one immediately,
	// regardless of the current file size.
	ForceRotate() error
}

// logStats. Supports regular log rotation.
type logStats struct {
	fileName  string
	sizeLimit int
	numFiles  int
	tsFormat  string

	lock        sync.Mutex
	sz          int
	w           SyncWriteCloser
	durable     bool
	closed      bool
	fileHandler FileHandler
}

// Create new LogStats object.
// Paramters:
// fileName:  Name of the log file. If the file name does not have ".log"
//
//	extension, it will be added internally - and the final log
//	file will have the ".log" extension.
//
// sizeLimit: Size limit for one file. It is not a hard limit. A single
//
//	log message cannot cross the log file boundary. So, as long
//	as the current file has not reached its size limit, the
//	incoming log message will be written to the current file.
//	This can lead to log files larger than sizeLimit.
//
// numFiles:  Number of log files to be maintained.
// tsFormat:  Format in which the timestamps in the log messages are
//
//	to be logged.
func NewLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*logStats, error) {
	return NewLogStatsWithFileHandler(fileName, sizeLimit, numFiles, tsFormat, nil)
}

// NewLogStatsWithFileHandler creates logstats with an optional file handler.
// When fileHandler is nil, the built-in handler (gzip rotation) is used.
func NewLogStatsWithFileHandler(fileName string, sizeLimit int, numFiles int, tsFormat string, fileHandler FileHandler) (*logStats, error) {
	var err error
	fileName, err = validateInput(fileName, numFiles)
	if err != nil {
		return nil, err
	}

	if fileHandler == nil {
		fileHandler = newDefaultFileHandler()
	}

	w, sz, err := fileHandler.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &logStats{
		fileName:    fileName,
		sizeLimit:   sizeLimit,
		numFiles:    numFiles,
		tsFormat:    tsFormat,
		w:           w,
		sz:          sz,
		fileHandler: fileHandler,
	}, nil
}

func (lst *logStats) SetDurable(durable bool) {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	lst.durable = durable
}

func (lst *logStats) ForceRotate() error {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	return lst.doRotate()
}

// doRotate performs the actual rotation. Caller must hold lst.lock.
func (lst *logStats) doRotate() error {
	if err := lst.w.Close(); err != nil {
		return err
	}

	w, sz, err := lst.fileHandler.Rotate(lst.fileName, lst.numFiles)
	if err != nil {
		return err
	}
	lst.w = w
	lst.sz = sz
	return nil
}

func (lst *logStats) rotateIfNeeded() error {
	if !lst.needsRotation() {
		return nil
	}

	if DEBUG != 0 {
		fmt.Println("Log file", lst.fileName, "needs rotation")
	}

	return lst.doRotate()
}

func (lst *logStats) writeAndCommit(bytes []byte) error {
	_, err := lst.w.Write(bytes)
	if err != nil {
		return err
	}
	lst.sz += len(bytes)

	if lst.durable {
		err = lst.w.Sync()
	}

	return err
}

func (lst *logStats) Write(statType string, statMap map[string]interface{}) error {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	if lst.closed {
		return fmt.Errorf("Use of closed logStats object")
	}

	if err := lst.rotateIfNeeded(); err != nil {
		return err
	}

	bytes, err := lst.getBytesToWrite(statType, statMap)
	if err != nil {
		return err
	}

	return lst.writeAndCommit(bytes)
}

func (lst *logStats) getBytesToWrite(statType string, statMap map[string]interface{}) ([]byte, error) {
	bytes, err := json.Marshal(statMap)
	if err != nil {
		return nil, err
	}

	return lst.formatBytes(statType, bytes), nil
}

func (lst *logStats) formatBytes(statType string, bytes []byte) []byte {
	bytes = append(bytes, byte(10))

	prefix := []byte(strings.Join([]string{time.Now().Format(lst.tsFormat), statType, ""}, " "))
	bytes = append(prefix, bytes...)
	return bytes
}

func (lst *logStats) needsRotation() bool {
	return lst.sz >= lst.sizeLimit
}

func (lst *logStats) disableCompression() {
	lst.lock.Lock()
	defer lst.lock.Unlock()
	lst.fileHandler.DisableCompression()
}

func (lst *logStats) Close() {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	if lst.closed {
		return
	}

	if lst.w != nil {
		lst.w.Close()
	}

	lst.w = nil
	lst.closed = true
}

// dedupeLogStats. Supports log rotation. Stats get deduplicated across
// consecutive log messages of same type. This can save a lot of space
// but it comes with a cost that the individual log message cannot be
// consumed as-is. Deduplication resets on log rotation.
type dedupeLogStats struct {
	*logStats

	fileName  string
	sizeLimit int
	numFiles  int
	tsFormat  string

	lock     sync.Mutex
	sz       int
	durable  bool
	compress bool

	prevStatsMap map[string]map[string]interface{}
}

// Create new DedupeLogStats object.
// Paramters:
// fileName:  Name of the log file. If the file name does not have ".log"
//
//	extension, it will be added internally - and the final log
//	file will have the ".log" extension.
//
// sizeLimit: Size limit for one file. It is not a hard limit. A single
//
//	log message cannot cross the log file boundary. So, as long
//	as the current file has not reached its size limit, the
//	incoming log message will be written to the current file.
//	This can lead to log files larger than sizeLimit.
//
// numFiles:  Number of log files to be maintained.
// tsFormat:  Format in which the timestamps in the log messages are
//
//	to be logged.
func NewDedupeLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*dedupeLogStats, error) {
	return NewDedupeLogStatsWithFileHandler(fileName, sizeLimit, numFiles, tsFormat, nil)
}

// NewDedupeLogStatsWithFileHandler creates dedupe logstats with an optional
// file handler. When fileHandler is nil, the built-in handler is used.
func NewDedupeLogStatsWithFileHandler(fileName string, sizeLimit int, numFiles int, tsFormat string, fileHandler FileHandler) (*dedupeLogStats, error) {
	lStats, err := NewLogStatsWithFileHandler(fileName, sizeLimit, numFiles, tsFormat, fileHandler)
	if err != nil {
		return nil, err
	}

	return &dedupeLogStats{
		logStats:     lStats,
		fileName:     lStats.fileName,
		sizeLimit:    lStats.sizeLimit,
		numFiles:     lStats.numFiles,
		tsFormat:     lStats.tsFormat,
		sz:           lStats.sz,
		prevStatsMap: make(map[string]map[string]interface{}),
	}, nil
}

func (dlst *dedupeLogStats) Write(statType string, statMap map[string]interface{}) error {
	dlst.lock.Lock()
	defer dlst.lock.Unlock()
	// lst.lock must also be held: ForceRotate and Close hold lst.lock while
	// modifying lst.f/lst.w/lst.sz/lst.closed, and the embedded calls below
	// (needsRotation, rotateIfNeeded, writeAndCommit) access those fields
	// without going through lst's own locking path.
	dlst.logStats.lock.Lock()
	defer dlst.logStats.lock.Unlock()

	if dlst.closed {
		return fmt.Errorf("Use of closed dedupeLogStats object")
	}

	var bytes []byte
	var err error
	if dlst.needsRotation() {
		dlst.resetPrevStatsMap()
		bytes, err = dlst.logStats.getBytesToWrite(statType, statMap)

	} else {
		prevMap, ok := dlst.prevStatsMap[statType]
		if !ok {
			bytes, err = dlst.logStats.getBytesToWrite(statType, statMap)
		} else {
			filteredMap := make(map[string]interface{})
			populateFilteredMap(prevMap, statMap, filteredMap)
			bytes, err = dlst.logStats.getBytesToWrite(statType, filteredMap)
		}
	}

	if err != nil {
		return err
	}

	dlst.prevStatsMap[statType] = statMap

	if err = dlst.rotateIfNeeded(); err != nil {
		return err
	}

	return dlst.writeAndCommit(bytes)

}

func (dlst *dedupeLogStats) resetPrevStatsMap() {
	dlst.prevStatsMap = make(map[string]map[string]interface{})
}

// Compile-time interface checks.
var _ LogStats = (*logStats)(nil)
var _ LogStats = (*dedupeLogStats)(nil)

var gStatLogger LogStats
var gStatLoggerLock = sync.Mutex{}

func SetGlobalStatLogger(sLogger LogStats) {
	gStatLoggerLock.Lock()
	defer gStatLoggerLock.Unlock()

	gStatLogger = sLogger
}

func GetGlobalStatLogger() LogStats {
	gStatLoggerLock.Lock()
	defer gStatLoggerLock.Unlock()

	return gStatLogger
}
