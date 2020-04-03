package logstats

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
}

type logStats struct {
	fileName  string
	sizeLimit int
	numFiles  int

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
//
func NewLogStats(fileName string, sizeLimit int, numFiles int) (*logStats, error) {
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

	// TODO: Do we need to append newline char?
	bytes, err := json.Marshal(statMap)
	if err != nil {
		return err
	}

	f := lst.f

	// Rotate the logs only if current size of log file is more than
	// specified sizeLimit. This can lead to files larger than
	// sizeLimit.
	if lst.sz >= lst.sizeLimit {
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
