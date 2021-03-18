package index

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"
)

type DataLog interface {
	ReadLogItem(offset int64) (logItem *LogItem, err error)
	AddLogItem(logItem LogItem) (offset int64, err error)
}

type LogItem struct {
	key   string
	value string
	size  int64
}

func (l *LogItem) Key() string {
	return l.key
}

func (l *LogItem) Value() string {
	return l.value
}

func (l *LogItem) Size() int64 {
	return l.size
}

func NewLogItem(key string, value string) LogItem {
	size := int64(len([]byte(value)))
	return LogItem{key, value, size}
}

type LocalDataLog struct {
	flushThreshold int
	filePath       string
	buffer         []LogItem
}

func NewLocalDataLog(filePath string) DataLog {
	buffer := make([]LogItem, 0, 10)
	dataLog := LocalDataLog{10, filePath, buffer}
	return &dataLog
}

func (l *LocalDataLog) ReadLogItem(offset int64) (logItem *LogItem, err error) {
	storeFile, err := os.Open(l.filePath)

	if err != nil {
		return nil, err
	}

	defer storeFile.Close()

	_, err = storeFile.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(storeFile)
	record, err := reader.Read()

	if err != nil {
		return nil, err
	}

	key := record[0]
	value := record[1]
	s := record[2]
	size, parseError := strconv.ParseInt(s, 10, 64)

	if parseError != nil {
		return nil, errors.New(fmt.Sprintf("Could not convert size to int for offset %d", offset))
	}

	li := NewLogItem(key, value)
	li.size = size
	return &li, nil
}

func (l *LocalDataLog) AddLogItem(logItem LogItem) (offset int64, err error) {
	file, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}

	defer file.Close()

	length, write_err := file.WriteString(fmt.Sprintf("%s,%s,%d,\n", logItem.Key(), logItem.Value(), logItem.Size()))

	if write_err != nil {
		return 0, write_err
	}

	fi, statErr := file.Stat()
	if statErr != nil {
		return 0, statErr
	}

	offset = fi.Size() - int64(length)
	return offset, nil
}
