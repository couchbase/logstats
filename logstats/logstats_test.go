package logstats

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLogStatsBasics(t *testing.T) {
	DEBUG = 1

	// Create a stats logger
	tmpDir := os.TempDir()
	fileName := filepath.Join(tmpDir, "basics.log")

	err := cleanup([]string{fileName})

	var statLogger LogStats
	statLogger, err = NewLogStats(fileName, 32*1024*1024, 2, "2006-01-02T15:04:05.000-07:00")
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	// Write a stat
	stat := make(map[string]interface{})
	stat["k1"] = 10
	stat["k2"] = "Value2"

	k3stat := make(map[string]interface{})
	k3stat["k31"] = 310
	k3stat["k32"] = "Value32"
	stat["k3"] = k3stat
	err = statLogger.Write("kStats", stat)
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	exp := make([]map[string]interface{}, 0)
	vstat := make(map[string]interface{})
	vstat["type"] = "kStats"
	vstat["stat"] = stat

	exp = append(exp, vstat)

	// Verify stats
	err = verifyStats(exp, fileName)
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}
}

func cleanup(paths []string) error {
	for _, p := range paths {
		name := p[:len(p)-4]
		pattern := fmt.Sprintf("%s.*.log", name)
		all, err := filepath.Glob(pattern)
		if err != nil {
			return err
		}

		for _, name := range all {
			err := os.RemoveAll(name)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func verifyStats(exp []map[string]interface{}, fileName string) error {
	fileName = getLogFileName(fileName, 0)
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}

	var finfo os.FileInfo
	finfo, err = f.Stat()
	if err != nil {
		return err
	}

	buf := make([]byte, finfo.Size())
	_, err = f.Read(buf)
	if err != nil {
		return err
	}

	s := string(buf)
	lines := strings.Split(s, "\n")

	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}

	if len(lines) != len(exp) {
		return fmt.Errorf("Unexpected number of lines in the log file, exp %v actual %v",
			len(exp), len(lines))
	}

	for i, line := range lines {
		comps := strings.SplitN(line, " ", 3)
		if len(comps) != 3 {
			return fmt.Errorf("Unrecognised stat format")
		}

		ex := exp[i]
		if comps[1] != ex["type"] {
			return fmt.Errorf("Log type mismatch on line number %v, exp %v actual %v",
				i, ex["type"], comps[1])
		}

		m := make(map[string]interface{})
		err = json.Unmarshal([]byte(comps[2]), &m)
		if err != nil {
			return err
		}

		convertFloatsToInts(m)

		equal := reflect.DeepEqual(ex["stat"], m)
		if !equal {
			fmt.Printf("%v %T\n", m["k1"], m["k1"])
			return fmt.Errorf("Expected and actual stats are not equal. exp %v, %T actual %v, %T",
				ex["stat"], ex["stat"], m, m)
		}
	}

	return nil
}

func convertFloatsToInts(m map[string]interface{}) {
	for k, v := range m {
		vf, ok := v.(float64)
		if ok {
			m[k] = int(vf)
		}

		var vm map[string]interface{}

		vm, ok = v.(map[string]interface{})
		if ok {
			convertFloatsToInts(vm)
		}
	}
}
