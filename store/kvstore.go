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

type Index struct {
	LastOffset int64       `json:"lastOffset"`
	KeyOffsets []KeyOffset `json:"keyOffsets"`
}

type KeyOffset struct {
	Key    string `json:"key"`
	Offset int64  `json:"offset"`
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
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	offset, err := WritePut(path, key, value)
	if err != nil {
		return err
	}

	k.IndexCache.Add(key, offset)
	k.Cache.Add(key, value)
	k.logBufferChannel <- Command{PUT_COMMAND, key, value}

	return nil
}

func (k KvStore) Get(key string) (value string, err error) {
	v, ok := k.Cache.Get(key)

	if ok == false {
		offset, ok := k.IndexCache.Get(key)

		if ok != true {
			return "", errors.New("Unable to find offset in cache.")
		}

		off, check := offset.(int64)
		if !check {
			return "", errors.New("Offset is in inproper format.")
		}
		path := filepath.Join(".", STORAGE_DIR)
		path = filepath.Join(path, STORAGE_FILE)

		value, err = ReadGet(path, off)
	} else {
		value = fmt.Sprintf("%v", v)
	}

	return value, err
}

func (k *KvStore) Del(key string) error {
	_, ok := k.IndexCache.Get(key)
	k.Cache.Remove(key)
	k.IndexCache.Remove(key)

	log.Infof("Delete called for key %s")
	if ok {
		k.logBufferChannel <- Command{DEL_COMMAND, key, ""}
	}

	return nil
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
	indexCacheCopy, _ := NewSimpleCache()

	if cErr != nil {
		log.Fatal("Could not create cache for kv store.")
	}

	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	offset, loadErr := LoadIndex(indexCache)
	for _, key := range indexCache.Keys() {
		value, _ := indexCache.Get(key)
		indexCacheCopy.Add(key, value)
	}

	if loadErr != nil {
		log.Fatal("Could not load data into offset cache.")
	}

	cache, _ := NewLruCache()
	indexBuffer := make(chan KvPair, INDEX_FLUSH_THRESHOLD)
	logBuffer := make(chan Command, LOG_FLUSH_THRESHOLD)
	done := make(chan bool)

	go FlushLog(logBuffer, indexBuffer)
	go FlushIndex(indexCacheCopy, indexBuffer, done)

	return &KvStore{offset, cache, indexCache, indexBuffer, logBuffer, done}
}

func getMaxOffset(indexCache Cache) int64 {
	var maxOffset int64 = 0
	for _, key := range indexCache.Keys() {
		value, _ := indexCache.Get(key)
		offset, _ := value.(int64)
		if offset > maxOffset {
			maxOffset = offset
		}
	}

	return maxOffset
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

			for _, pair := range pairs {
				if !pair.Tomb && pair.Key != "" {
					initCache.Add(pair.Key, pair.Offset)
				} else {
					initCache.Remove(pair.Key)
				}
			}

			err := WriteIndex(initCache, swap_path)
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

func WriteIndex(indexCache Cache, filepath string) error {
	maxOffset := getMaxOffset(indexCache)
	index := Index{maxOffset, make([]KeyOffset, 0, len(indexCache.Keys()))}
	for _, key := range indexCache.Keys() {
		if key != "" {
			value, _ := indexCache.Get(key)
			offsetValue, _ := value.(int64)
			keyOffset := KeyOffset{key, offsetValue}
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

func FlushLog(logBuffer chan Command, indexBuffer chan KvPair) {
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	var commands []Command = make([]Command, 0, 10)
	for {
		command, ok := <-logBuffer
		commands = append(commands, command)

		if len(commands) == LOG_FLUSH_THRESHOLD || !ok {
			log.Infof("Log items flushing, threshold %d met or shutdown signal given.", LOG_FLUSH_THRESHOLD)
			for _, cmd := range commands {
				if cmd.Type == PUT_COMMAND {
					offset, err := WritePut(path, cmd.Key, cmd.Value)
					if err != nil {
						log.Fatal("Could not flush log!")
					}

					indexBuffer <- KvPair{cmd.Key, false, offset}
				} else if cmd.Type == DEL_COMMAND {
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

func ReadGet(filePath string, offset int64) (string, error) {
	storeFile, openErr := os.Open(filePath)

	if openErr != nil {
		return "", openErr
	}

	_, seekErr := storeFile.Seek(offset, 0)
	if seekErr != nil {
		return "", openErr
	}

	reader := csv.NewReader(storeFile)
	log.Infoln("Reading persistent file.")
	record, err := reader.Read()

	if err != nil {
		storeFile.Close()
		return "", err
	}

	value := record[1]
	storeFile.Close()

	return value, nil
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
	var offset int64 = 0

	if fileExists(path) {
		log.Info("Index data found loading from disk.")
		offset, err = LoadIndexJson(cache, path)
	}

	if err != nil {
		return 0, err
	}

	log.Info("Reading any missing data from log on disk.")

	path = filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	return LoadIndexData(offset, cache, path)
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
		cache.Add(kv.Key, kv.Offset)
	}

	return lastLineOffset, nil
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
			cache.Add(key, position)
		} else {
			log.Info("Tombstone detected removing key from index.")
			cache.Remove(key)
		}

		position += int64(len(lineBytes))
	}

	log.Infoln("Successfully Read persistent file into cache with offsets.")
	return position, err
}
