package logstats

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	MAX_NUM_FILES = 99
)

var DEBUG int = 0

//
// LogStats interface
//
type LogStats interface {

	// Write stats to the file.
	Write(statType string, statMap map[string]interface{}) error

	// Set flag for durability - when set to true, each call to Write will
	// also call os.File.Sync()
	SetDurable(durable bool)

	// Closes the log file if open.
	Close()
}

//
// logStats. Supports regular log rotation.
//
type logStats struct {
	fileName  string
	sizeLimit int
	numFiles  int
	tsFormat  string

	lock     sync.Mutex
	sz       int
	f        *os.File
	durable  bool
	compress bool
	closed   bool
}

//
// Create new LogStats object.
// Paramters:
// fileName:  Name of the log file. If the file name does not have ".log"
//            extension, it will be added internally - and the final log
//            file will have the ".log" extension.
// sizeLimit: Size limit for one file. It is not a hard limit. A single
//            log message cannot cross the log file boundary. So, as long
//            as the current file has not reached its size limit, the
//            incoming log message will be written to the current file.
//            This can lead to log files larger than sizeLimit.
// numFiles:  Number of log files to be maintained.
// tsFormat:  Format in which the timestamps in the log messages are
//            to be logged.
//
func NewLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*logStats, error) {
	var err error
	fileName, err = validateInput(fileName, numFiles)
	if err != nil {
		return nil, err
	}

	f, sz, err := openLogFile(fileName)
	if err != nil {
		return nil, err
	}

	lst := &logStats{
		fileName:  fileName,
		sizeLimit: sizeLimit,
		numFiles:  numFiles,
		tsFormat:  tsFormat,
		f:         f,
		sz:        sz,
		compress:  true,
	}
	return lst, nil
}

func (lst *logStats) SetDurable(durable bool) {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	lst.durable = durable
}

func (lst *logStats) rotateIfNeeded() error {
	// Rotate the logs only if current size of log file is more than
	// specified sizeLimit. This can lead to files larger than
	// sizeLimit.
	if lst.needsRotation() {
		if DEBUG != 0 {
			fmt.Println("Log file", lst.fileName, "needs rotation")
		}

		err := lst.f.Close()
		if err != nil {
			return err
		}

		f, sz, err := rotate(lst.fileName, lst.numFiles, lst.compress)
		if err != nil {
			return err
		}
		lst.f = f
		lst.sz = sz
	}

	return nil
}

func (lst *logStats) writeAndCommit(bytes []byte) error {
	f := lst.f

	err := writeToFile(f, bytes)
	if err != nil {
		return err
	}
	lst.sz += len(bytes)

	if lst.durable {
		err = f.Sync()
	}

	return err
}

func (lst *logStats) Write(statType string, statMap map[string]interface{}) error {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	if lst.closed {
		return fmt.Errorf("Use of closed logStats object")
	}

	err := lst.rotateIfNeeded()
	if err != nil {
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

	lst.compress = false
}

func (lst *logStats) Close() {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	if lst.closed {
		return
	}

	if lst.f != nil {
		lst.f.Close()
	}

	lst.f = nil
	lst.closed = true
}

//
// dedupeLogStats. Supports log rotation. Stats get deduplicated across
// consecutive log messages of same type. This can save a lot of space
// but it comes with a cost that the individual log message cannot be
// consumed as-is. Deduplication resets on log rotation.
//
type dedupeLogStats struct {
	*logStats

	fileName  string
	sizeLimit int
	numFiles  int
	tsFormat  string

	lock     sync.Mutex
	sz       int
	f        *os.File
	durable  bool
	compress bool

	prevStatsMap map[string]map[string]interface{}
}

//
// Create new LogStats object.
// Paramters:
// fileName:  Name of the log file. If the file name does not have ".log"
//            extension, it will be added internally - and the final log
//            file will have the ".log" extension.
// sizeLimit: Size limit for one file. It is not a hard limit. A single
//            log message cannot cross the log file boundary. So, as long
//            as the current file has not reached its size limit, the
//            incoming log message will be written to the current file.
//            This can lead to log files larger than sizeLimit.
// numFiles:  Number of log files to be maintained.
// tsFormat:  Format in which the timestamps in the log messages are
//            to be logged.
//
func NewDedupeLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*dedupeLogStats, error) {

	var err error
	fileName, err = validateInput(fileName, numFiles)
	if err != nil {
		return nil, err
	}

	f, sz, err := openLogFile(fileName)
	if err != nil {
		return nil, err
	}

	lStats := &logStats{
		fileName:  fileName,
		sizeLimit: sizeLimit,
		numFiles:  numFiles,
		tsFormat:  tsFormat,
		f:         f,
		sz:        sz,
		compress:  true,
	}

	lst := &dedupeLogStats{
		logStats:     lStats,
		fileName:     fileName,
		sizeLimit:    sizeLimit,
		numFiles:     numFiles,
		tsFormat:     tsFormat,
		f:            f,
		sz:           sz,
		compress:     true,
		prevStatsMap: make(map[string]map[string]interface{}),
	}
	return lst, nil
}

func (dlst *dedupeLogStats) Write(statType string, statMap map[string]interface{}) error {
	dlst.lock.Lock()
	defer dlst.lock.Unlock()

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

	dlst.prevStatsMap[statType] = statMap

	err = dlst.rotateIfNeeded()
	if err != nil {
		return err
	}

	return dlst.writeAndCommit(bytes)

}

func (dlst *dedupeLogStats) resetPrevStatsMap() {
	dlst.prevStatsMap = make(map[string]map[string]interface{})
}
