package bucketclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"
)

type Seekables interface {
	~[]Bucket | ~[]Object
}

// Seekable interface to control seeking and caching
type Seekable[T Seekables] interface {
	SetParams(params url.Values)
	GetLastAccessed() time.Time
	GetLastFetched() time.Time
	GetCache() map[int]T
	GetOffset() int
	GetLimit() int
	Seek(offset int)
	Clear()
	SetLimit(limit int)
	Next() (T, error)
	GetData() (T, error)
}

// Seeker struct to hold seeking parameters and data
type seeker[T Seekables] struct {
	db           *BucketDB
	lastAccessed time.Time
	lastFetched  time.Time
	limit        int
	offset       int
	cache        map[int]T
	endpoint     string
	params       url.Values
	mu           sync.Mutex
}

func (s *seeker[T]) SetParams(params url.Values) {
	s.params = params
}

func (s *seeker[T]) GetLastAccessed() time.Time {
	return s.lastAccessed
}

func (s *seeker[T]) GetLastFetched() time.Time {
	return s.lastFetched
}

func (s *seeker[T]) GetCache() map[int]T {
	return s.cache
}

func (s *seeker[T]) GetOffset() int {
	return s.offset
}

func (s *seeker[T]) GetLimit() int {
	return s.limit
}

func (s *seeker[T]) Seek(offset int) {
	s.offset = offset
}

// Clear clears the cache but retains limit and offset
func (s *seeker[T]) Clear() {
	s.cache = make(map[int]T)
}

func (s *seeker[T]) SetLimit(limit int) {
	s.limit = limit
}

// Next fetches and loads the next possible data from the API
func (s *seeker[T]) Next() (T, error) {
	data, err := s.loadData()
	if err != nil {
		return data, err
	}

	if len(data) < 1 { // Empty data without the error(s)
		return data, errors.New("no more data to load")
	}

	s.offset += len(data) // Seek to the data end
	return data, err
}

// GetData returns the cached data or calls Next
func (s *seeker[T]) GetData() (T, error) {
	s.lastAccessed = time.Now()
	s.mu.Lock()
	if data, ok := s.cache[s.offset]; ok { // Includes potentially partial data
		s.mu.Unlock() // Must unlock
		return data, nil
	}
	s.mu.Unlock()

	return s.Next()
}

// loadData fetches the data from the API and caches it
func (s *seeker[T]) loadData() (T, error) {
	var result T
	s.lastFetched = time.Now()

	var params = url.Values{}
	if s.params != nil {
		params = s.params
	}

	// Overwrites the limit|offset params on default ones
	params.Set("limit", fmt.Sprintf("%d", s.limit-s.offset%s.limit)) // Controlled buffering (offset) within the s.limit block
	params.Set("offset", fmt.Sprintf("%d", s.offset))

	data, err := s.db.apiV1Request(METHOD_GET, fmt.Sprintf("%s?%s", s.endpoint, params.Encode()), nil, nil)
	if err != nil {
		return result, err
	}

	// Unmarshal data into the Seekables type (Bucket or Object)
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}

	// Cache the result
	if len(result) > 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cache[s.offset] = result // segmental caching
	}

	return result, nil
}

// Creates new Seekable Instance and initializes it
func newSeekable[T Seekables](db *BucketDB, endpoint string, params url.Values) Seekable[T] {
	return &seeker[T]{
		db:       db,
		limit:    10,
		offset:   0,
		cache:    make(map[int]T),
		endpoint: endpoint,
		params:   params,
		mu:       sync.Mutex{},
	}
}
