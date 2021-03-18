package index

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"sort"
)

type IndexItem struct {
	partialKey string
	offset     int64
	size       int64
}

func NewIndexItem(key string, offset int64, size int64) IndexItem {
	pk := getPartialKey(key)
	return IndexItem{pk, offset, size}
}

func (i *IndexItem) Size() int64 {
	return i.size
}

func (i *IndexItem) PartialKey() string {
	return i.partialKey
}

func (i *IndexItem) Offset() int64 {
	return i.offset
}

type Index interface {
	Get(key string) (indexItems []IndexItem, ok bool)
	Put(key string, indexItem IndexItem)
	Del(key string)
	DataLog() DataLog
	Save() error
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

func (i *LocalIndex) Save() error {
	var items []IndexItem
	iitems := i.indexItems
	for _, value := range iitems {
		for _, it := range value {
			items = append(items, it)
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Offset() < items[j].Offset()
	})

	fileName := fmt.Sprintf("index_swap%d.csv", rand.Int())
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, item := range items {
		var record []string
		record = append(record, item.PartialKey())
		record = append(record, string(item.Offset()))
		record = append(record, string(item.Size()))

		writer.Write(record)
	}

	writer.Flush()

	err = os.Rename(fileName, i.storageFilePath)

	return err
}

func (i *LocalIndex) Get(key string) (indexItems []IndexItem, ok bool) {
	partialKey := getPartialKey(key)
	indexItems, ok = i.indexItems[partialKey]
	return indexItems, ok
}

func (i *LocalIndex) Put(indexItem IndexItem) {
	indexItems, ok := i.indexItems[indexItem.PartialKey()]
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

		logItem, err := i.localDataLog.ReadLogItem(item.Offset())
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

	//Load index if exist

	return &localIndex
}
