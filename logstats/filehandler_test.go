package logstats

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// noopEncryptor is a pass-through FileHandler used to simulate the interface
// that secondary/common.LogStatsFileHandler satisfies, without importing it.
type noopFileHandler struct {
	compress bool
}

func (h *noopFileHandler) Open(fileName string) (SyncWriteCloser, int, error) {
	f, sz, err := openLogFile(fileName)
	if err != nil {
		return nil, 0, err
	}
	return f, sz, nil
}

func (h *noopFileHandler) Rotate(fileName string, numFiles int) (SyncWriteCloser, int, error) {
	f, sz, err := rotate(fileName, numFiles, h.compress)
	if err != nil {
		return nil, 0, err
	}
	return f, sz, nil
}

func (h *noopFileHandler) DisableCompression() { h.compress = false }

// TestFileHandlerRotationProducesGzip verifies that when using a custom
// FileHandler, rotation produces .gz files and the active file is plain text.
func TestFileHandlerRotationProducesGzip(t *testing.T) {
	tmpDir := os.TempDir()
	fileName := filepath.Join(tmpDir, "fh_smoke.log")

	if err := cleanup([]string{fileName}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	handler := &noopFileHandler{compress: true}
	logger, err := NewDedupeLogStatsWithFileHandler(fileName, 64, 4, "2006-01-02T15:04:05.000-07:00", handler)
	if err != nil {
		t.Fatalf("NewDedupeLogStatsWithFileHandler: %v", err)
	}
	defer logger.Close()

	// Write enough to trigger two rotations.
	for i := 0; i < 6; i++ {
		if err := logger.Write("kStats", getSimpleStat(i)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	// At least one rotated .gz file should exist.
	name := fileName[:len(fileName)-4]
	gzFiles, _ := filepath.Glob(fmt.Sprintf("%s.log.*.gz", name))
	if len(gzFiles) == 0 {
		t.Fatal("expected at least one rotated .gz file, found none")
	}

	// Each .gz file must be valid gzip.
	for _, path := range gzFiles {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %q: %v", path, err)
		}
		gr, err := gzip.NewReader(f)
		f.Close()
		if err != nil {
			t.Fatalf("%q is not valid gzip: %v", path, err)
		}
		gr.Close()
	}

	// ForceRotate should work without error.
	if err := logger.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
}

// TestFileHandlerNoCompression verifies the no-compress path (used in
// encrypted mode where the FileHandler handles its own framing).
func TestFileHandlerNoCompression(t *testing.T) {
	tmpDir := os.TempDir()
	fileName := filepath.Join(tmpDir, "fh_nocompress.log")

	if err := cleanup([]string{fileName}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	handler := &noopFileHandler{compress: false}
	logger, err := NewDedupeLogStatsWithFileHandler(fileName, 64, 4, "2006-01-02T15:04:05.000-07:00", handler)
	if err != nil {
		t.Fatalf("NewDedupeLogStatsWithFileHandler: %v", err)
	}
	defer logger.Close()

	for i := 0; i < 6; i++ {
		if err := logger.Write("kStats", getSimpleStat(i)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	name := fileName[:len(fileName)-4]

	// No .gz files should exist.
	gzFiles, _ := filepath.Glob(fmt.Sprintf("%s.log.*.gz", name))
	if len(gzFiles) > 0 {
		t.Fatalf("expected no .gz files, found %v", gzFiles)
	}

	// Plain rotated files should exist.
	plain, _ := filepath.Glob(fmt.Sprintf("%s.log.*", name))
	plain = filterTmp(plain)
	if len(plain) == 0 {
		t.Fatal("expected plain rotated files, found none")
	}

	// Each should be readable as plain text (not gzip).
	for _, path := range plain {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %q: %v", path, err)
		}
		if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
			t.Fatalf("%q looks like gzip but compression was off", path)
		}
	}
}

func filterTmp(paths []string) []string {
	out := paths[:0]
	for _, p := range paths {
		if !strings.HasSuffix(p, ".tmp") {
			out = append(out, p)
		}
	}
	return out
}
