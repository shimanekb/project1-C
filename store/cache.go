package kvstore

import (
	"fmt"
	lru "github.com/hashicorp/golang-lru"
)

type Cache interface {
	Get(key string) (value interface{}, ok bool)
	Add(key string, value interface{})
	Remove(key string)
	Keys() []string
}

type LruCache struct {
	Lru *lru.ARCCache
}

func (l *LruCache) Add(key string, value interface{}) {
	l.Lru.Add(key, value)
}

func (l *LruCache) Get(key string) (value interface{}, ok bool) {
	var v interface{}
	v, ok = l.Lru.Get(key)
	value = fmt.Sprintf("%v", v)
	return value, ok
}

func (l *LruCache) Remove(key string) {
	l.Lru.Remove(key)
}

func (l *LruCache) Keys() []string {
	return l.Keys()
}

func NewLruCache() (Cache, error) {
	var cache *lru.ARCCache
	cache, err := lru.NewARC(1000)
	return &LruCache{cache}, err
}
