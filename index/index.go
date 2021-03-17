package kvstore

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
	path := i.storageFilePath
	indexItems, ok := i.Get(key)

	if !ok {
		return
	}

	for index, item := range indexItems {
		k, err := readKey(path, item.offset)
		if err != nil {
			break
		}

		if key == k {
			indexItems[index] = indexItems[len(indexItems)-1]
			i.indexItems[getPartialKey(key)] = indexItems[:len(indexItems)-1]
			continue
		}
	}
}

func readKey(filepath string, offset int64) (key string, err error) {
}

func NewLocalIndex(storageFilePath string) Index {
	indexItems := make(map[string][]IndexItem)
	return &LocalIndex{storageFilePath, indexItems}
}
