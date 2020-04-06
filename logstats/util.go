package logstats

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func getLogFileName(fileName string, num int) string {
	// Assumption: fileName always has ".log" extention.

	name := fileName[:len(fileName)-4]
	numLen := len(fmt.Sprintf("%d", MAX_NUM_FILES))
	numLenFormat := fmt.Sprintf("%%0%dd", numLen)
	format := fmt.Sprintf("%%s.%s.%%s", numLenFormat)
	return fmt.Sprintf(format, name, num, "log")
}

func getLogFileNumber(fileName string) (int, error) {
	names := strings.Split(fileName, ".")
	if len(names) < 3 {
		return 0, fmt.Errorf("Unexpected log file name")
	}

	return strconv.Atoi(names[len(names)-2])
}

func openLogFile(fileName string) (*os.File, int, error) {
	// Assumption: fileName always has ".log" extention.

	dir := filepath.Dir(fileName)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, 0, err
	}

	fname := getLogFileName(fileName, 0)
	flag := os.O_CREATE | os.O_APPEND | os.O_WRONLY
	var f *os.File
	f, err = os.OpenFile(fname, flag, 0755)
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

func rotate(fileName string, numFiles int) (*os.File, int, error) {
	// Assumption: fileName always has ".log" extention.

	name := fileName[:len(fileName)-4]
	pattern := fmt.Sprintf("%s.*.log", name)
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

			newFname = getLogFileName(fileName, num)
		} else {
			newFname = all[i+1]
		}

		if DEBUG != 0 {
			fmt.Println("Renamaing oldfile", oldFname, "newfile", newFname)
		}

		err := os.Rename(oldFname, newFname)
		if err != nil {
			return nil, 0, err
		}
	}

	return openLogFile(fileName)
}
