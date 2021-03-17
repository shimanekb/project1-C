package kvstore

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const (
	INDEX_FLUSH_THRESHOLD int    = 100
	LOG_FLUSH_THRESHOLD   int    = 10
	STORAGE_DIR           string = "storage"
	STORAGE_FILE          string = "data_records.csv"
	INDEX_FILE            string = "index_file.json"
	INDEX_SWAP_FILE       string = "index_swap_file.json"
	GET_COMMAND           string = "get"
	PUT_COMMAND           string = "put"
	DEL_COMMAND           string = "del"
	TOMB_FLAG             string = "Tomb"
)

var flushLock sync.RWMutex = sync.RWMutex{}

type Index struct {
	LastOffset int64       `json:"lastOffset"`
	KeyOffsets []KeyOffset `json:"keyOffsets"`
}

type KeyOffset struct {
	Key     string  `json:"key"`
	Size    int     `json:"size"`
	Offsets []int64 `json:"offsets"`
}

type Store interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Del(key string) error
}

type Command struct {
	Type  string
	Key   string
	Value string
}

type KvPair struct {
	Key    string
	Tomb   bool
	Size   int
	Offset int64
}

// Add error if shutdown.
type KvStore struct {
	LastLineOffset     int64
	Cache              Cache
	IndexCache         Cache
	indexBufferChannel chan KvPair
	logBufferChannel   chan Command
	shutdownChannel    chan bool
}

func (k *KvStore) Shutdown() {
	close(k.logBufferChannel)
	log.Info("Shutting down kvStore saving any remaining data.")
	<-k.shutdownChannel
	log.Info("All data saved.")
}

func (k *KvStore) Put(key string, value string) error {
	k.Cache.Add(key, value)
	k.logBufferChannel <- Command{PUT_COMMAND, key, value}

	return nil
}

func ReadGet(path string, key string, offsets []int64) (string, string, error) {
	for _, off := range offsets {
		k, v, err := ReadKvItem(path, off)
		if err != nil {
			return "", "", err
		}

		if k == key {
			value := fmt.Sprintf("%v", v)
			return key, value, nil
		}
	}

	return "", "", errors.New("Unable to read key value.")
}

func (k KvStore) Get(key string) (string, error) {
	value, cacheOk := k.Cache.Get(key)

	if cacheOk {
		return fmt.Sprintf("%v", value), nil
	}

	log.Infof("Read for key %s was not in cache, reading disk", key)
	partialKey := getPartialKey(key)
	flushLock.RLock()
	offsets, ok := k.IndexCache.Get(partialKey)
	flushLock.RUnlock()

	if !ok {
		return "", errors.New("Offsets not in index!.")
	}
	offs, check := offsets.([]int64)
	if !check {
		return "", errors.New("Offset is in inproper format.")
	}

	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	_, v, err := ReadGet(path, key, offs)
	if err != nil {
		return "", err
	}

	return v, nil
}

func (k *KvStore) Del(key string) error {
	k.Cache.Remove(key)
	RemoveIndexItem(k.IndexCache, key)
	offsets, ok := k.IndexCache.Get(getPartialKey(key))
	if ok {
		offs, ok := offsets.([]int64)
		if len(offs) > 0 && ok {
			log.Infof("After removing off for key %s, %d", key, offs[0])
		} else if ok {
			log.Infof("After removing off for key %s, %d", key, -1)
		}
	}
	log.Infof("Delete called for key %s", key)
	k.logBufferChannel <- Command{DEL_COMMAND, key, ""}

	return nil
}

func getPartialKey(key string) string {
	partialKey := key
	if len(key) > 16 {
		partialKey = key[0:15]
	}

	return partialKey
}

func NewKvStore() *KvStore {
	log.Info("Creating new Kv Store.")

	log.Info("Creating storage directory if does not exist.")
	newpath := filepath.Join(".", STORAGE_DIR)
	err := os.MkdirAll(newpath, os.ModePerm)

	if err != nil {
		log.Fatalf("Cannot create directory for storage at %s", STORAGE_DIR)
	}
	log.Info("Created storage directory.")

	indexCache, cErr := NewSimpleCache()
	if cErr != nil {
		log.Fatal("Could not create cache for kv store.")
	}

	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	offset, loadErr := LoadIndex(indexCache)

	if loadErr != nil {
		log.Fatal("Could not load data into offset cache.")
	}

	cache, _ := NewLruCache()
	indexBuffer := make(chan KvPair, INDEX_FLUSH_THRESHOLD)
	logBuffer := make(chan Command, LOG_FLUSH_THRESHOLD)
	done := make(chan bool)

	go FlushLog(indexCache, logBuffer, indexBuffer)
	go FlushIndex(indexCache, indexBuffer, done)

	return &KvStore{offset, cache, indexCache, indexBuffer, logBuffer, done}
}

func FlushIndex(initCache Cache, indexBuffer chan KvPair, done chan bool) {
	path := filepath.Join(".", STORAGE_DIR)
	swap_path := filepath.Join(path, INDEX_SWAP_FILE)
	path = filepath.Join(path, INDEX_FILE)
	var pairs []KvPair = make([]KvPair, 0, 100)
	for {
		kvPair, ok := <-indexBuffer
		pairs = append(pairs, kvPair)

		if len(pairs) == INDEX_FLUSH_THRESHOLD || !ok {
			log.Info("Creating checkpoint for index.")

			if fileExists(swap_path) {
				log.Info("Swap file for index detected removing before creating new tmp index.")
				err := os.Remove(swap_path)

				if err != nil {
					log.Fatal("Could not delete detected swap index file.")
				}
			}

			lastOffset, _ := LoadIndexJson(initCache, path)
			for _, pair := range pairs {
				if !pair.Tomb && pair.Key != "" {
					lastOffset = pair.Offset
				}
			}

			err := WriteIndex(lastOffset, initCache, swap_path)
			if err != nil {
				log.Fatal("Could not open swap temp index file.")
			}

			pairs = make([]KvPair, 0, 100)

			log.Info("Swapping index file.")
			err = os.Rename(swap_path, path)

			if err != nil {
				log.Fatal("Could not swap index.")
			}

			log.Info("index items flushed")
		}

		if !ok {
			log.Info("Closing index flushing channel")
			done <- true
			break
		}
	}

}

func WriteIndex(maxOffset int64, indexCache Cache, filepath string) error {
	index := Index{maxOffset, make([]KeyOffset, 0, len(indexCache.Keys()))}

	log.Infof("Last offset is %d", maxOffset)
	for _, key := range indexCache.Keys() {
		if key != "" {
			value, _ := indexCache.Get(key)
			offsetValue, _ := value.([]int64)
			keyOffset := KeyOffset{key, size, offsetValue}
			index.KeyOffsets = append(index.KeyOffsets, keyOffset)

		}
	}

	file, err := json.MarshalIndent(index, "", " ")
	if err != nil {
		log.Fatal("Could not open swap temp index file.")
		return err
	}

	write_err := ioutil.WriteFile(filepath, file, 0644)

	if write_err != nil {
		log.Fatal("Unable to write cache (index) offset to start.")
		return write_err
	}

	return nil
}

func FlushLog(indexCache Cache, logBuffer chan Command, indexBuffer chan KvPair) {
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	var commands []Command = make([]Command, 0, 10)
	for {
		command, ok := <-logBuffer
		commands = append(commands, command)

		if len(commands) == LOG_FLUSH_THRESHOLD || !ok {
			log.Infof("Log items flushing, threshold %d met or shutdown signal given.", LOG_FLUSH_THRESHOLD)
			flushLock.Lock()
			for _, cmd := range commands {
				if cmd.Type == PUT_COMMAND {
					offset, err := WritePut(path, cmd.Key, cmd.Value)
					if err != nil {
						log.Fatal("Could not flush log!")
					}

					var size int = len([]byte(cmd.Value))
					AddIndexItem(indexCache, cmd.Key, offset)
					indexBuffer <- KvPair{cmd.Key, false, size, offset}
				}
			}
			flushLock.Unlock()
			for _, cmd := range commands {
				if cmd.Type == DEL_COMMAND {
					_, err := WritePut(path, cmd.Key, "")

					if err != nil {
						log.Fatal("Could not flush log!")
					}

					indexBuffer <- KvPair{cmd.Key, true, 0}
				}
			}

			commands = make([]Command, 0, 10)
			log.Info("Log items flushed")
		}

		if !ok {
			close(indexBuffer)
			log.Info("Shutting down, closed indexBuffer channel.")
			break
		}
	}
}

func WriteDelete(filePath string, key string, value string) (offset int64, err error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		return 0, err
	}

	length, write_err := file.WriteString(fmt.Sprintf("%s,%s,%s\n", key, value, TOMB_FLAG))
	fi, statErr := file.Stat()
	if statErr != nil {
		return 0, statErr
	}

	offset = fi.Size() - int64(length)
	return offset, write_err
}

func WritePut(filePath string, key string, value string) (offset int64, err error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	defer file.Close()
	if err != nil {
		return 0, err
	}

	length, write_err := file.WriteString(fmt.Sprintf("%s,%s,\n", key, value))
	fi, statErr := file.Stat()
	if statErr != nil {
		return 0, statErr
	}

	offset = fi.Size() - int64(length)
	return offset, write_err
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func LoadIndex(cache Cache) (lastLineOffset int64, err error) {
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, INDEX_FILE)
	lastLineOffset = 0

	if fileExists(path) {
		log.Info("Index data found loading from disk.")
		storeFile, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0644)
		if openErr != nil {
			return 0, openErr
		}
		defer storeFile.Close()

		byteValue, _ := ioutil.ReadAll(storeFile)
		var index Index
		json.Unmarshal(byteValue, &index)

		lastLineOffset = index.LastOffset
	}

	if err != nil {
		return 0, err
	}

	log.Info("Reading any missing data from log on disk.")

	path = filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	return LoadIndexData(lastLineOffset, cache, path)
}

func LoadIndexJson(cache Cache, filePath string) (lastLineOffset int64, err error) {
	storeFile, openErr := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, 0644)
	if openErr != nil {
		return 0, openErr
	}
	defer storeFile.Close()

	byteValue, _ := ioutil.ReadAll(storeFile)
	var index Index
	json.Unmarshal(byteValue, &index)

	lastLineOffset = index.LastOffset
	log.Infof("Last offset was %d", lastLineOffset)
	for _, kv := range index.KeyOffsets {
		for _, off := range kv.Offsets {
			AddIndexItem(cache, kv.Key, off)
		}
	}

	return lastLineOffset, nil
}

func ReadKvItem(filePath string, offset int64) (key string, value interface{}, err error) {
	storeFile, openErr := os.Open(filePath)

	if openErr != nil {
		return "", nil, openErr
	}

	_, seekErr := storeFile.Seek(offset, 0)
	if seekErr != nil {
		return "", nil, openErr
	}

	reader := csv.NewReader(storeFile)
	log.Infoln("Reading persistent file.")
	record, err := reader.Read()

	if err != nil {
		storeFile.Close()
		return "", nil, err
	}

	key = record[0]
	value = record[1]
	storeFile.Close()

	return key, value, nil
}

func RemoveIndexItem(cache Cache, key string) {
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	partialKey := getPartialKey(key)
	values, ok := cache.Get(partialKey)
	newOffsets := make([]int64, 0, 1)

	if partialKey != "" && ok {
		offsets, check := values.([]int64)
		if !check {
			log.Fatal("could not retrieve offsets from cache to remove index item.")
		}

		for _, offset := range offsets {
			k, _, err := ReadKvItem(path, offset)
			if err != nil {
				log.Fatal("Could not read kv item")
				break
			}

			if key == k {
				log.Infof("Found correct offset for key %s, removing offset %d", key, offset)
				continue
			} else {
				log.Infof("Adding checked offset non match key %s, offset %d", key, offset)
				newOffsets = append(newOffsets, offset)
			}
		}

		cache.Add(partialKey, newOffsets)
	}

}

func AddIndexItem(cache Cache, key string, offset int64) {
	partialKey := getPartialKey(key)
	values, ok := cache.Get(partialKey)

	if ok {
		log.Infof("offsets found in index cache for key %s", key)
		offsets, check := values.([]int64)
		if !check {
			log.Fatal("could not retrieve offsets from cache to add new index item.")
		}

		RemoveIndexItem(cache, key)
		values, _ = cache.Get(partialKey)
		offsets, _ = values.([]int64)

		offsets = append(offsets, offset)
		cache.Add(partialKey, offsets)
	} else {

		log.Infof("offsets not found in index cache for key %s, adding new offset", key)
		offsets := make([]int64, 0, 1)
		offsets = append(offsets, offset)
		cache.Add(partialKey, offsets)
	}

}

func LoadIndexData(startingOffset int64, cache Cache, filePath string) (lastLineOffset int64, err error) {
	storeFile, openErr := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)

	if openErr != nil {
		return 0, openErr
	}

	var buffer bytes.Buffer
	var position int64
	reader := io.TeeReader(storeFile, &buffer)
	csvReader := csv.NewReader(reader)
	_, seekErr := storeFile.Seek(startingOffset, 0)
	if seekErr != nil {
		return 0, seekErr
	}

	log.Infoln("Reading persistent file into cache with offsets.")
	for {
		record, readErr := csvReader.Read()
		if readErr == io.EOF {
			log.Info("End of file reached.")
			break
		}

		if err != nil {
			err = readErr
			break
		}

		lineBytes, _ := buffer.ReadBytes('\n')
		key := record[0]
		value := record[1]
		tomb := record[2]

		if value != tomb {
			AddIndexItem(cache, key, position)
		} else {
			log.Info("Tombstone detected removing key from index.")
			cache.Remove(key)
		}

		position += int64(len(lineBytes))
	}

	log.Infoln("Successfully Read persistent file into cache with offsets.")
	return position, err
}
