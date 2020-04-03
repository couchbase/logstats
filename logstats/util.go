package logstats

import (
	"fmt"
	"os"
	"path/filepath"
)

func getLogFileName(name string, num int) string {
	numLen := len(fmt.Sprintf("%d", MAX_NUM_FILES))
	numLenFormat := fmt.Sprintf("%%0%dd", numLen)
	format := fmt.Sprintf("%%s.%s.%%s", numLenFormat)
	return fmt.Sprintf(format, name, num, "log")
}

func openLogFile(fileName string) (*os.File, error) {
	dir := filepath.Dir()
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	// Remove .log from the file name
	name := fileName[:len(fileName)-4]

	fname := getLogFileName(name, 0)
	flag := os.O_CREATE | os.O_APPEND
	return os.OpenFile(fname, flag, 0755)
}

func writeToFile(f *os.File, bytes []byte) error {
	_, err := f.Write(bytes)
	return err
}

func rotate(fileName string, numFiles int) error {

}
