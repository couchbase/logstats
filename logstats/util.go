package logstats

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

//
// Utility functions for file handling
//
func getLogFileName(fileName string, num int, compress bool) string {
	// Assumption: fileName always has ".log" extention.

	name := fileName[:len(fileName)-4]
	numLen := len(fmt.Sprintf("%d", MAX_NUM_FILES))
	numLenFormat := fmt.Sprintf("%%0%dd", numLen)
	format := fmt.Sprintf("%%s.%s.%%s", numLenFormat)
	fname := fmt.Sprintf(format, name, num, "log")
	if compress && num > 0 {
		fname = fname + ".gz"
	}
	return fname
}

func getLogFileNumber(fileName string) (int, error) {
	names := strings.Split(fileName, ".")
	if len(names) < 3 {
		return 0, fmt.Errorf("Unexpected log file name")
	}

	idx := len(names) - 2
	if names[len(names)-1] == "gz" {
		idx = len(names) - 3
	}

	return strconv.Atoi(names[idx])
}

func openLogFile(fileName string) (*os.File, int, error) {
	// Assumption: fileName always has ".log" extention.

	dir := filepath.Dir(fileName)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, 0, err
	}

	fname := getLogFileName(fileName, 0, false)
	flag := os.O_CREATE | os.O_APPEND | os.O_WRONLY
	var f *os.File
	f, err = os.OpenFile(fname, flag, 0744)
	if err != nil {
		return nil, 0, err
	}

	var finfo os.FileInfo
	finfo, err = f.Stat()
	if err != nil {
		return nil, 0, err
	}

	if DEBUG != 0 {
		fmt.Println("Opened log file", fname)
	}

	return f, int(finfo.Size()), nil
}

func writeToFile(f *os.File, bytes []byte) error {
	n, err := f.Write(bytes)
	if DEBUG != 0 {
		fmt.Println(n, "bytes written to the file")
	}

	return err
}

func rotate(fileName string, numFiles int, compress bool) (*os.File, int, error) {
	// Assumption: fileName always has ".log" extention.

	name := fileName[:len(fileName)-4]
	var pattern string
	if compress {
		pattern = fmt.Sprintf("%s.*.log.gz", name)
	} else {
		pattern = fmt.Sprintf("%s.*.log", name)
	}

	all, err := filepath.Glob(pattern)
	if err != nil {
		return nil, 0, err
	}

	sort.Strings(all)
	l := len(all)
	for i := l - 1; i >= 0; i-- {
		var newFname string

		oldFname := all[i]
		if i == l-1 {
			num, err := getLogFileNumber(all[i])
			if err != nil {
				return nil, 0, err
			}

			num = num + 1
			if num >= numFiles {
				continue
			}

			newFname = getLogFileName(fileName, num, compress)
		} else {
			newFname = all[i+1]
		}

		if DEBUG != 0 {
			fmt.Println("Renaming oldfile", oldFname, "newfile", newFname)
		}

		err := os.Rename(oldFname, newFname)
		if err != nil {
			return nil, 0, err
		}
	}

	if compress {
		// compress filname.0.log to filename.1.log.gz
		sourceFname := getLogFileName(fileName, 0, compress)
		targetFname := getLogFileName(fileName, 1, compress)
		err = compressFile(sourceFname, targetFname)
		if err != nil {
			return nil, 0, err
		}

		err = os.Remove(sourceFname)
		if err != nil {
			return nil, 0, err
		}
	}

	return openLogFile(fileName)
}

func compressFile(sourceFname, targetFname string) error {
	flags := os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	f, err := os.OpenFile(targetFname, flags, 0744)
	if err != nil {
		return err
	}

	writer := gzip.NewWriter(f)

	var r *os.File
	r, err = os.Open(sourceFname)
	if err != nil {
		return err
	}

	var finfo os.FileInfo
	finfo, err = r.Stat()
	if err != nil {
		return err
	}

	buf := make([]byte, finfo.Size())
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	if DEBUG != 0 {
		fmt.Println("compressFile: Read", len(buf), "bytes from the file:", sourceFname)
	}

	_, err = writer.Write(buf)
	if err != nil {
		return err
	}

	if DEBUG != 0 {
		fmt.Println("compressFile: Written", len(buf), "bytes to the file:", targetFname)
	}

	err = r.Close()
	if err != nil {
		return err
	}

	return writer.Close()
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
			continue
		}

		if equalInt64(v, prev) {
			continue
		}

		if equalBool(v, prev) {
			continue
		}

		if equalUint64(v, prev) {
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

func equalUint64(v, prev interface{}) bool {
	var vint, prevint uint64
	var ok bool

	vint, ok = v.(uint64)
	if !ok {
		return false
	}

	prevint, ok = prev.(uint64)
	if !ok {
		return false
	}

	return vint == prevint
}

func equalBool(v, prev interface{}) bool {
	var vbool, prevbool bool
	var ok bool

	vbool, ok = v.(bool)
	if !ok {
		return false
	}

	prevbool, ok = prev.(bool)
	if !ok {
		return false
	}

	return vbool == prevbool
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
