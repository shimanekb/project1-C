package kvstore

import (
	"github.com/shimanekb/project1-C/index"
	log "github.com/sirupsen/logrus"
	"math"
)

const (
	INDEX_FLUSH_THRESHOLD int    = 100
	LOG_FLUSH_THRESHOLD   int    = 10
	GET_COMMAND           string = "get"
	PUT_COMMAND           string = "put"
	DEL_COMMAND           string = "del"
)

type Command struct {
	Type  string
	Key   string
	Value string
}

type Store interface {
	Put(key string, value string) error
	Get(key string) (value string, ok bool)
	Del(key string)
	Flush()
}

type LocalStore struct {
	idx           index.Index
	cache         Cache
	commandBuffer []Command
	commandCount  int
}

func (s *LocalStore) Get(key string) (value string, ok bool) {
	s.commandCount = s.commandCount + 1
	val, ok := s.cache.Get(key)
	if ok {
		value = val.(string)
		return value, ok
	}

	indexItems, ok := s.idx.Get(key)
	if !ok {
		return "", ok
	}

	dl := s.idx.DataLog()
	//TODO make concurrent
	for _, item := range indexItems {
		litem, err := dl.ReadLogItem(item.Offset())
		if err != nil {
			value = ""
			ok = false
			break
		}

		if litem.Key() == key {
			value = litem.Value()
			ok = true
			break
		}
	}

	s.flush()
	return value, ok
}

func (s *LocalStore) flushLog() {
	log.Info("Flushing log.")
	ids := s.idx
	dl := s.idx.DataLog()
	for _, cmd := range s.commandBuffer {
		if cmd.Type != DEL_COMMAND {
			li := index.NewLogItem(cmd.Key, cmd.Value, -1)
			offset, err := dl.AddLogItem(li)
			if err != nil {
				break
			}

			iitem := index.NewIndexItem(cmd.Key, offset, li.Size())
			ids.Put(iitem)
		}
	}
	s.commandBuffer = make([]Command, 0, LOG_FLUSH_THRESHOLD)
}

func (s *LocalStore) flushIndex() {
	log.Info("Flushing index.")
	ids := s.idx
	ids.Save()

	s.commandCount = 0
}

func (s *LocalStore) Flush() {
	s.flushLog()
	s.flushIndex()
}

func (s *LocalStore) flush() {
	cnt := float64(s.commandCount)
	logthresh := float64(LOG_FLUSH_THRESHOLD)
	idxthresh := float64(INDEX_FLUSH_THRESHOLD)
	if math.Mod(cnt, logthresh) == 0 {
		s.flushLog()
	}

	if math.Mod(cnt, idxthresh) == 0 {
		s.flushIndex()
	}
}

func (s *LocalStore) Del(key string) {
	s.commandCount = s.commandCount + 1
	s.cache.Remove(key)
	s.idx.Del(key)
	s.commandBuffer = append(s.commandBuffer, Command{DEL_COMMAND, key, ""})
	s.flush()
}

func (s *LocalStore) Put(key string, value string) error {
	s.commandCount = s.commandCount + 1
	s.cache.Add(key, value)
	s.commandBuffer = append(s.commandBuffer, Command{PUT_COMMAND, key, value})
	s.flush()

	return nil
}

func NewLocalStore(dataLogPath string, indexPath string) (Store, error) {
	dl := index.NewLocalDataLog(dataLogPath)
	idx := index.NewLocalIndex(indexPath, dl)
	err := idx.Load()
	if err != nil {
		return nil, err
	}

	cache, _ := NewLruCache()
	buffer := make([]Command, 0, LOG_FLUSH_THRESHOLD)

	store := LocalStore{idx, cache, buffer, 0}

	return &store, nil
}
