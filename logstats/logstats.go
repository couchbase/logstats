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

	// Write stats to the file, without deduplication.
	Write(statType string, statMap map[string]interface{}) error

	// Set flag for durability - when set to true, each call to Write will
	// also call os.File.Sync()
	SetDurable(durable bool)

	// Write Deduplicated stats to the file
	WriteDedupe(statType string, statMap map[string]interface{}) error
}

//
// logStats. Supports regular log rotation.
//
type logStats struct {
	fileName  string
	sizeLimit int
	numFiles  int
	tsFormat  string

	lock    sync.Mutex
	sz      int
	f       *os.File
	durable bool
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

		f, sz, err := rotate(lst.fileName, lst.numFiles)
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

func (lst *logStats) WriteDedupe(statType string, statMap map[string]interface{}) error {
	return fmt.Errorf("WriteDedupe is not supported with object type %T", lst)
}

func (lst *logStats) getBytesToWrite(statType string, statMap map[string]interface{}) ([]byte, error) {
	bytes, err := json.Marshal(statMap)
	if err != nil {
		return nil, err
	}

	return lst.formatBytes(statType, bytes), nil
}

func (lst *logStats) formatBytes(statType string, bytes []byte) []byte {
	bytes = append(bytes, []byte("\n")...)

	prefix := []byte(strings.Join([]string{time.Now().Format(lst.tsFormat), statType, ""}, " "))
	bytes = append(prefix, bytes...)
	return bytes
}

func (lst *logStats) needsRotation() bool {
	return lst.sz >= lst.sizeLimit
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

	lock    sync.Mutex
	sz      int
	f       *os.File
	durable bool

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
		f:         f,
		sz:        sz,
	}

	lst := &dedupeLogStats{
		logStats:     lStats,
		fileName:     fileName,
		sizeLimit:    sizeLimit,
		numFiles:     numFiles,
		f:            f,
		sz:           sz,
		prevStatsMap: make(map[string]map[string]interface{}),
	}
	return lst, nil
}

func (dlst *dedupeLogStats) WriteDedupe(statType string, statMap map[string]interface{}) error {
	// return dlst.Write(statType, statMap)

	dlst.lock.Lock()
	defer dlst.lock.Unlock()

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

//
// Input validation functions
//
func validateInput(fileName string, numFiles int) (string, error) {
	if numFiles > MAX_NUM_FILES {
		return fileName, fmt.Errorf("NewLogStats: More than %v files not supported.", MAX_NUM_FILES)
	}

	if numFiles < 1 {
		return fileName, fmt.Errorf("NewLogStats: Unsupported file count %v", numFiles)
	}

	if !strings.HasSuffix(fileName, ".log") {
		fileName = fileName + ".log"
	}

	return fileName, nil
}

//
// Utility funtions needed for filtering
//
func populateFilteredMap(prevMap, currMap, newMap map[string]interface{}) {
	for k, v := range currMap {
		prev, ok := prevMap[k]
		if !ok {
			newMap[k] = v
		}

		if equalInt64(v, prev) {
			continue
		}

		if equalStrings(v, prev) {
			continue
		}

		var currM, prevM map[string]interface{}
		currM, ok = v.(map[string]interface{})
		if !ok {
			newMap[k] = v
			continue
		}

		prevM, ok = prev.(map[string]interface{})
		if !ok {
			newMap[k] = v
			continue
		}

		newM := make(map[string]interface{})
		newMap[k] = newM
		populateFilteredMap(prevM, currM, newM)
		if len(newM) == 0 {
			delete(newMap, k)
		}
	}
}

func equalInt64(v, prev interface{}) bool {
	var vint, prevint int64
	var ok bool

	vint, ok = v.(int64)
	if !ok {
		return false
	}

	prevint, ok = prev.(int64)
	if !ok {
		return false
	}

	return vint == prevint
}

func equalStrings(v, prev interface{}) bool {
	var vstr, prevstr string
	var ok bool

	vstr, ok = v.(string)
	if !ok {
		return false
	}

	prevstr, ok = prev.(string)
	if !ok {
		return false
	}

	return vstr == prevstr
}
