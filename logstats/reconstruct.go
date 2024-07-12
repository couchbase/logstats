package logstats

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// omits new line from input
func parseBufferTillNewLine(sourceBuffer []byte) (bool, []byte) {
	var outputBuffer = make([]byte, 0, 1024)
	for _, c := range sourceBuffer {
		if c == '\n' {
			return true, outputBuffer
		}
		outputBuffer = append(outputBuffer, c)
	}
	return false, outputBuffer
}

func extractStatsFromLine(source []byte) int {
	var end int
	var stack = make([]byte, 0)
	var stackTop = -1
	for end = len(source) - 1; end > 0; end-- {
		switch source[end] {
		case '}', ']', ')':
			stack = append(stack, source[end])
			stackTop++
		case '{':
			if stack[stackTop] != '}' {
				fmt.Printf(
					"invalid line: bracket mismatch error - expected closing '}' but got '%v'. Input - %s\n",
					string(stack[stackTop]),
					source,
				)
				return -1
			}
			stack = stack[:stackTop]
			stackTop--
		case '[':
			// need ')' as well because histogram gets printed as [)
			if stack[stackTop] != ']' && stack[stackTop] != ')' {
				fmt.Printf(
					"invalid line: bracket mismatch error - expected closing ']'/')' but got '%v'. Input - %s\n",
					string(stack[stackTop]),
					source,
				)
				return -1
			}
			stack = stack[:stackTop]
			stackTop--
		case '(':
			if stack[stackTop] != ')' && stack[stackTop] != ']' {
				fmt.Printf(
					"invalid line: bracket mismatch error - expected closing ')'/']' but got '%v'. Input - %s\n",
					string(stack[stackTop]),
					source,
				)
				return -1
			}
			stack = stack[:stackTop]
			stackTop--
		}
		// only exit with a valid ans
		if len(stack) == 0 {
			return end
		}
	}
	return -1
}

// returns the key in reverse format
func getStatNameFromSource(end int, source []byte) string {
	if end <= 0 {
		return ""
	}

	var lastSpaceIndex int = 0
	var statName strings.Builder

	for ; end > 0; end-- {
		if source[end] == ' ' {
			lastSpaceIndex = end
		}
		statName.WriteByte(source[end])
	}

	return string(statName.String()[:statName.Len()-lastSpaceIndex])
}

func ReconstructStatLine(keyToStatsMap map[string]interface{}, source []byte) []byte {
	if !isValidStatLine(source) {
		return source
	}

	var defaultAns = source
	if keyToStatsMap == nil {
		return defaultAns
	}
	var statStart = extractStatsFromLine(source)

	if statStart == -1 {
		fmt.Printf("failed to extract valid json in stats for line:\n\t%s\n", string(source))
		return defaultAns
	}

	if source[statStart-1] != ' ' {
		fmt.Printf("messed up stat map - %s\n", string(source))
		return defaultAns
	}

	var statKey = getStatNameFromSource(statStart-2, source)

	var statMap = make(map[string]interface{})
	var err = json.Unmarshal(source[statStart:], &statMap)
	if err != nil {
		fmt.Printf("failed to unmarshal stats into map with err - %v\n\tstat source - %v\n",
			err, source)
		return defaultAns
	}

	var prevStatMap map[string]interface{}
	if prevStatInterface, keyExists := keyToStatsMap[statKey]; !keyExists {
		keyToStatsMap[statKey] = statMap
		return defaultAns
	} else {
		prevStatMap = prevStatInterface.(map[string]interface{})
	}

	for key, stat := range prevStatMap {
		if _, keyExists := statMap[key]; !keyExists {
			statMap[key] = stat
		}
	}

	keyToStatsMap[statKey] = statMap

	var newReconstructedStatBytes []byte
	newReconstructedStatBytes, err = json.Marshal(statMap)
	if err != nil {
		fmt.Printf("failed to reconstruct %v-%v with err - \n\t%v\n", statKey, statMap, err)
		return defaultAns
	}

	var ans = make([]byte, statStart, statStart+len(newReconstructedStatBytes)+1)
	copy(ans, defaultAns[:statStart])
	ans = append(ans, newReconstructedStatBytes...)
	return ans
}

func isValidStatLine(source []byte) bool {
	if len(source) == 0 {
		return false
	}
	var firstChar = int(source[0]) - int('0')
	if firstChar >= 0 && firstChar <= 9 {
		return true
	}
	return false
}

func ReconstructStatFile(sourceFile, outputFile *os.File) error {
	var fileReadBuffer = make([]byte, 1024)
	var lineBuffer = make([]byte, 0)
	var offset = 0
	var n int
	var totalLines = 0
	var keyToStatsMap = make(map[string]interface{})

	var closeWait sync.WaitGroup
	var lineCh = make(chan []byte, 10_000)
	var outCh = make(chan []byte, 10_000)

	var globalErr error

	// parser
	closeWait.Add(1)
	go func() {
		defer closeWait.Done()
		for line := range lineCh {
			var outputBuffer = ReconstructStatLine(keyToStatsMap, line)

			if outputBuffer != nil {
				outputBuffer = append(outputBuffer, '\n')

				outCh <- outputBuffer

			} else {
				fmt.Printf("parsed full line %v\n", string(line))
			}
		}

		close(outCh)
	}()

	// writer
	closeWait.Add(1)
	go func() {
		var err error

		defer closeWait.Done()

		for outputBuffer := range outCh {
			_, err = outputFile.Write(outputBuffer)
			if err != nil {
				globalErr = fmt.Errorf(
					"failed to write to dest file %v with err %v",
					outputFile.Name(),
					err,
				)
				break

			}

			totalLines++
			if totalLines%10_000 == 0 {
				_ = outputFile.Sync()
				if totalLines != 10_000 {
					// deletes previous line
					fmt.Printf("\033[1A\033[K")
				}
				fmt.Printf("%v stat lines parsed\n", totalLines)
			}
		}

		fmt.Printf("total lines parsed - %v\n", totalLines)
	}()

	// reader
	var err error
	for {
		n, err = sourceFile.ReadAt(fileReadBuffer, int64(offset))
		if err != nil && err != io.EOF {
			globalErr = err
			break
		}
		var parsedBuffer []byte
		var completeLine bool

		for len(fileReadBuffer) > 0 {
			completeLine, parsedBuffer = parseBufferTillNewLine(fileReadBuffer)
			var lenOfCharsParsed = len(parsedBuffer)

			lineBuffer = append(lineBuffer, parsedBuffer...)

			if completeLine {
				// len of parsed buffer + 1 because we also read `\n`
				lenOfCharsParsed++

				lineCh <- lineBuffer

				lineBuffer = make([]byte, 0)
			}

			fileReadBuffer = fileReadBuffer[lenOfCharsParsed:]
		}

		fileReadBuffer = make([]byte, 1024)
		offset += n
		if err == io.EOF {
			break
		}
	}

	close(lineCh)
	closeWait.Wait()

	return globalErr
}
