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
	fileName string
	fileSize int
	numFiles int

	lock    sync.Mutex
	sz      int
	f       *os.File
	durable bool
}

func NewLogStats(fileName string, fileSize int, numFiles int) (*logStats, error) {
	if numFiles > MAX_NUM_FILES {
		return nil, fmt.Errorf("NewLogStats: More than %v files not supported.", MAX_NUM_FILES)
	}

	if numFiles < 1 {
		return nil, fmt.Errorf("NewLogStats: Unsupported file count", numFiles)
	}

	if !strings.HasSuffix(fileName, ".log") {
		fileName = filepath.Join(fileName, ".log")
	}

	f, err := openLogFile(fileName)
	if err != nil {
		return nil, err
	}

	lst := &logStats{
		fileName: fileName,
		fileSize: fileSize,
		numFiles: numFiles,
		f:        f,
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

	bytes, err := json.Marshal(statMap)
	if err != nil {
		return error
	}

	f := lst.f
	if len(bytes)+sz >= lst.fileSize {
		f, err := rotate(lst.fileName)
		if err != nil {
			return err
		}
	}

	return writeToFile(f, bytes)
}
