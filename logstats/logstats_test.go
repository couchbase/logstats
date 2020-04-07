package logstats

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestLogStatsBasics(t *testing.T) {
	// Create a stats logger
	tmpDir := os.TempDir()
	fileName := filepath.Join(tmpDir, "basics.log")

	err := cleanup([]string{fileName})
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	var statLogger LogStats
	statLogger, err = NewLogStats(fileName, 32*1024*1024, 2, "2006-01-02T15:04:05.000-07:00")
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	// Write a stat
	stat := getSimpleStat(0)
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

func TestLogStatsRotation(t *testing.T) {
	// Create a stats logger
	tmpDir := os.TempDir()
	fileName := filepath.Join(tmpDir, "rotation.log")

	err := cleanup([]string{fileName})
	if err != nil {
		t.Fatalf("TestLogStatsRotation failed with error %v", err)
	}

	var statLogger LogStats
	statLogger, err = NewLogStats(fileName, 128, 4, "2006-01-02T15:04:05.000-07:00")
	if err != nil {
		t.Fatalf("TestLogStatsRotation failed with error %v", err)
	}

	// Write a stat
	exp := make([]map[string]interface{}, 0)
	for i := 0; i < 5; i++ {
		stat := getSimpleStat(i)
		err = statLogger.Write("kStats", stat)
		if err != nil {
			t.Fatalf("TestLogStatsRotation failed with error %v", err)
		}

		vstat := make(map[string]interface{})
		vstat["type"] = "kStats"
		vstat["stat"] = stat
		exp = append(exp, vstat)
	}

	// Verify stats
	err = verifyStats(exp, fileName)
	if err != nil {
		t.Fatalf("TestLogStatsRotation failed with error %v", err)
	}
}

func TestDedupeLogStatsBasics(t *testing.T) {
	DEBUG = 1

	// Create a stats logger
	tmpDir := os.TempDir()
	fileName := filepath.Join(tmpDir, "dedupe_basics.log")

	err := cleanup([]string{fileName})
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	var statLogger LogStats
	statLogger, err = NewDedupeLogStats(fileName, 1024*1024, 2, "2006-01-02T15:04:05.000-07:00")
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	// Write dedupe stats
	stat := getSimpleStat(0)
	err = statLogger.WriteDedupe("kStats", stat)
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	exp := make([]map[string]interface{}, 0)
	vstat := make(map[string]interface{})
	vstat["type"] = "kStats"
	vstat["stat"] = stat

	exp = append(exp, vstat)

	stat = getSimpleStat(0)
	stat["k1"] = int64(9876)
	err = statLogger.WriteDedupe("kStats", stat)
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}

	estat := make(map[string]interface{})
	estat["k1"] = int64(9876)
	vstat = make(map[string]interface{})
	vstat["type"] = "kStats"
	vstat["stat"] = estat
	exp = append(exp, vstat)

	// Verify stats
	err = verifyStats(exp, fileName)
	if err != nil {
		t.Fatalf("TestLogStatsBasics failed with error %v", err)
	}
}

func getSimpleStat(seed int) map[string]interface{} {
	stat := make(map[string]interface{})
	stat["k1"] = int64(seed + 10)
	stat["k2"] = fmt.Sprintf("Value%v", seed+2)

	k3stat := make(map[string]interface{})
	k3stat["k31"] = int64(300*seed + 10)
	k3stat["k32"] = fmt.Sprintf("Value%v", 30*seed+2)
	stat["k3"] = k3stat
	return stat
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

func getAllLogsFromFiles(fileName string) ([]string, error) {

	name := fileName[:len(fileName)-4]
	pattern := fmt.Sprintf("%s.*.log", name)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	sort.Strings(files)
	all := make([]string, 0)
	for i := len(files) - 1; i >= 0; i-- {
		all = append(all, files[i])
	}

	lines := make([]string, 0)
	for _, fname := range all {
		f, err := os.Open(fname)
		if err != nil {
			return nil, err
		}

		var finfo os.FileInfo
		finfo, err = f.Stat()
		if err != nil {
			return nil, err
		}

		buf := make([]byte, finfo.Size())
		_, err = f.Read(buf)
		if err != nil {
			return nil, err
		}

		s := string(buf)
		flines := strings.Split(s, "\n")

		if len(flines[len(flines)-1]) == 0 {
			flines = flines[:len(flines)-1]
		}
		lines = append(lines, flines...)
	}

	return lines, nil
}

func verifyStats(exp []map[string]interface{}, fileName string) error {

	lines, err := getAllLogsFromFiles(fileName)
	if err != nil {
		return err
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
			m[k] = int64(vf)
		}

		var vm map[string]interface{}

		vm, ok = v.(map[string]interface{})
		if ok {
			convertFloatsToInts(vm)
		}
	}
}
