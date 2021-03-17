package kvstore

import (
	"encoding/csv"
	"fmt"
	"os"
)

type DataLog interface {
	Read(offset int64) (logItem *LogItem, err error)
	Add(key string, value string) (offset int64, err error)
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

func NewLogItem(key string, value string, size int64) LogItem {
	return LogItem{key, value, size}
}

type LocalDataLog struct {
	flushThreshold int
	filePath       string
}

func NewLocalDataLog(filePath string) DataLog {
	dataLog := LocalDataLog{10, filePath}
	return &dataLog
}

func (l *LocalDataLog) Read(offset int64) (logItem *LogItem, err error) {
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
	size := int64(len([]byte(value)))

	li := NewLogItem(key, value, size)
	return &li, nil
}

func (l *LocalDataLog) Add(key string, value string) (offset int64, err error) {
	file, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		return 0, err
	}

	size := int64(len([]byte(value)))
	length, write_err := file.WriteString(fmt.Sprintf("%s,%s,%d,\n", key, value, size))

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

type IndexItem struct {
	offset int64
	size   int64
}

func NewIndexItem(offset int64, size int64) IndexItem {
	return IndexItem{offset, size}
}

func (i *IndexItem) Size() int64 {
	return i.size
}

func (i *IndexItem) Offset() int64 {
	return i.offset
}

type Index interface {
	Get(key string) (indexItems []IndexItem, ok bool)
	Put(key string, indexItem IndexItem)
	Del(key string)
	DataLog() DataLog
}

func getPartialKey(key string) string {
	partialKey := key
	if len(key) > 16 {
		partialKey = key[0:15]
	}

	return partialKey
}

type LocalIndex struct {
	storageFilePath string
	indexItems      map[string][]IndexItem
	localDataLog    DataLog
}

func (i *LocalIndex) Get(key string) (indexItems []IndexItem, ok bool) {
	partialKey := getPartialKey(key)
	indexItems, ok = i.indexItems[partialKey]
	return indexItems, ok
}

func (i *LocalIndex) Put(key string, indexItem IndexItem) {
	indexItems, ok := i.indexItems[key]
	if !ok {
		indexItems = make([]IndexItem, 0)
	}

	indexItems = append(indexItems)
}

func (i *LocalIndex) Del(key string) {
	indexItems, ok := i.Get(key)

	if !ok {
		return
	}

	for index, item := range indexItems {

		logItem, err := i.localDataLog.Read(item.Offset())
		if err != nil {
			break
		}

		if logItem.Key() == key {
			indexItems[index] = indexItems[len(indexItems)-1]
			i.indexItems[getPartialKey(key)] = indexItems[:len(indexItems)-1]
			continue
		}
	}
}

func NewLocalIndex(storageFilePath string, dataLog DataLog) Index {
	indexItems := make(map[string][]IndexItem)
	localIndex := LocalIndex{storageFilePath, indexItems, dataLog}

	return &localIndex
}
