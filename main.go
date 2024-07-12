package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/couchbase/logstats/logstats"
)

func main() {
	var sourceStatPath = flag.String("reconstruct-stat-file", "", "absolute/relative path to the source stat file")
	flag.Parse()
	if sourceStatPath == nil || len(*sourceStatPath) == 0 {
		panic("invalid value of parameter `file_path`. please retry with a valid value")
	}
	var err error
	*sourceStatPath, err = filepath.Abs(*sourceStatPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get absolute path for stat file with err - %v", err))
	}

	var sourceFile *os.File
	sourceFile, err = os.OpenFile(*sourceStatPath, os.O_RDONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Unable to open source stat file %v. err - %v", sourceStatPath, err))
	}
	defer sourceFile.Close()

	var dir, fileName = filepath.Split(*sourceStatPath)
	fileName, _ = strings.CutSuffix(fileName, filepath.Ext(fileName))
	var outputPath = filepath.Join(dir, fileName+"_duped.log")
	var outputFile *os.File
	outputFile, err = os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("Unable to create dest file at %v with err %v", outputPath, err))
	}
	defer outputFile.Close()

	err = logstats.ReconstructStatFile(sourceFile, outputFile)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Stats file reconstructed and saved at %v\n", outputPath)
}
