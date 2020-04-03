package logstats

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	MAX_NUM_FILES = 99
)

//
// LogStats interface
//
type LogStats interface {

	// Write stats to the file
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
// tsFormat
func NewLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*logStats, error) {
	if numFiles > MAX_NUM_FILES {
		return nil, fmt.Errorf("NewLogStats: More than %v files not supported.", MAX_NUM_FILES)
	}

	if numFiles < 1 {
		return nil, fmt.Errorf("NewLogStats: Unsupported file count", numFiles)
	}

	if !strings.HasSuffix(fileName, ".log") {
		fileName = filepath.Join(fileName, ".log")
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

func (lst *logStats) Write(statType string, statMap map[string]interface{}) error {
	lst.lock.Lock()
	defer lst.lock.Unlock()

	bytes, err := lst.getBytesToWrite(statType, statMap)

	f := lst.f

	// Rotate the logs only if current size of log file is more than
	// specified sizeLimit. This can lead to files larger than
	// sizeLimit.
	if lst.needsRotation() {
		f, sz, err := rotate(lst.fileName)
		if err != nil {
			return err
		}
		lst.f = f
		lst.sz = sz
	}

	err = writeToFile(f, bytes)
	if err != nil {
		return err
	}
	lst.sz += len(bytes)

	if lst.durable {
		err = f.Sync()
	}

	return err
}

func (lst *logStats) WriteDedupe(statType string, statMap map[string]interface{}) error {
	return fmt.Errorf("WriteDedupe is not supported with object type %T", lst)
}

func (lst *logStats) getBytesToWrite(statType string, statMap map[string]interface{}) ([]byte, error) {
	bytes, err := json.Marshal(statMap)
	if err != nil {
		return nil, err
	}

	return lst.formatBytes(bytes), nil
}

func (lst *logStats) formatBytes(bytes []byte) []byte {
	// TODO: This function generates a lot of garbage. Can we use sync.Pool?
	// Do we need sync.Pool?

	bytes = append(bytes, []byte("\n")...)

	prefix := []byte(strings.Join([]string{time.Now().Format(lst.tsFormat), statType, ""}, " "))
	bytes = append(prefix, bytes...)
	return bytes, nil
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
//
func NewDedupeLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*dedupeLogStats, error) {
	if numFiles > MAX_NUM_FILES {
		return nil, fmt.Errorf("NewLogStats: More than %v files not supported.", MAX_NUM_FILES)
	}

	if numFiles < 1 {
		return nil, fmt.Errorf("NewLogStats: Unsupported file count", numFiles)
	}

	if !strings.HasSuffix(fileName, ".log") {
		fileName = filepath.Join(fileName, ".log")
	}

	f, sz, err := openLogFile(fileName)
	if err != nil {
		return nil, err
	}

	lst := &dedupeLogStats{
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
	return dlst.Write(statType, statMap)
}

func (dlst *dedupeLogStats) resetPrevStatsMap() {
	dlst.prevStatsMap = make(map[string]map[string]interface{})
}

func (dlst *dedupeLogStats) getBytesToWrite(statType string, statMap map[string]interface{}) ([]byte, error) {
	if dlst.needsRotation() {
		dlst.resetPrevStatsMap()
		return dlst.logStats.getBytesToWrite(statType, statMap)
	}

	var bytes []byte
	var err error

	prevMap, ok := dlst.prevStatsMap[statType]
	if !ok {
		bytes, err = dlst.logStats.getBytesToWrite(statType, statMap)
		dlst.prevStatsMap[statType] = statMap
	} else {
		bytes, err = dlst.getFilteredBytes(prevStat, statMap)
	}

	return bytes, err
}

func (dlst *dedupeLogStats) getFilteredBytes(prevMap, currMap map[string]interface{}) map[string]interface{} {
	newMap := make(map[string]interface{})
	for k, v := range currMap {
		prev, ok := prevMap[k]
		if !ok {
			newMap[k] = v
		}

		if equalIntegers(v, prev) {
			continue
		}

		if equalStrings(v, prev) {
			continue
		}

		// Data type of the value is unsupported for filtering.
		newMap[k] = v
	}

	return newMap
}

//
// Utility funtions needed for filtering
//

func equalIntegers(v, prev interface{}) bool {
	var vint, prevint int
	var ok bool

	vint, ok = v.(int)
	if !ok {
		return false
	}

	prevint, ok = prev.(int)
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
