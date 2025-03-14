package swiftds

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"

	swift "github.com/ncw/swift"
)

type SwiftContainer struct {
	conn  *swift.Connection
	cache *QueryCache

	Config
}

type QueryCache struct {
	lock   sync.RWMutex
	prefix string
	index  int
	name   string
}

func (c *QueryCache) Invalidate() {
	c.lock.Lock()
	c.name = ""
	c.lock.Unlock()
}

type Config struct {
	swift.Connection
	Container string
}

func NewSwiftDatastore(conf Config) (*SwiftContainer, error) {
	c := &conf.Connection

	if err := c.Authenticate(); err != nil {
		return nil, err
	}

	_, _, err := c.Container(conf.Container)
	if err != nil {
		return nil, err
	}

	return &SwiftContainer{
		conn:   c,
		cache:  &QueryCache{},
		Config: conf,
	}, nil
}

// Bulk APIs does not allow names starting with "/",
// so we need to normalize them here.
func keyToName(k ds.Key) string {
	return strings.TrimPrefix(k.String(), "/")
}

func (s *SwiftContainer) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	data, err := s.conn.ObjectGetBytes(s.Container, keyToName(k))
	switch err {
	case nil:
		return data, nil
	case swift.ObjectNotFound:
		return nil, ds.ErrNotFound
	default:
		return nil, err
	}
}

func (s *SwiftContainer) Delete(ctx context.Context, k ds.Key) error {
	s.cache.Invalidate()
	return s.conn.ObjectDelete(s.Container, keyToName(k))
}

func (s *SwiftContainer) Put(ctx context.Context, k ds.Key, val []byte) error {
	s.cache.Invalidate()
	return s.conn.ObjectPutBytes(s.Container, keyToName(k), val, "application/octet-stream")
}

func (s *SwiftContainer) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, _, err := s.conn.Object(s.Container, keyToName(k))
	switch err {
	case nil:
		return true, nil
	case swift.ObjectNotFound:
		return false, nil
	default:
		return false, err
	}
}

func (s *SwiftContainer) GetSize(ctx context.Context, k ds.Key) (int, error) {
	info, _, err := s.conn.Object(s.Container, keyToName(k))

	if err != nil {
		switch err {
		case swift.ObjectNotFound:
			return 0, ds.ErrNotFound
		default:
			return 0, err
		}
	}

	maxInt := int64((^uint(0)) >> 1)
	if info.Bytes > maxInt {
		return 0, fmt.Errorf("integer overflow")
	}
	return int(info.Bytes), nil
}

func (s *SwiftContainer) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("swiftds doesnt support filters or orders")
	}

	opts := swift.ObjectsOpts{
		Prefix: strings.TrimPrefix(q.Prefix, "/"),
		// Number of entries to fetch at once
		Limit: 10000,
	}

	offset := q.Offset

	s.cache.lock.RLock()
	if s.cache.prefix == opts.Prefix && s.cache.name != "" && s.cache.index <= offset {
		opts.Marker = s.cache.name
		offset = s.cache.index - offset
	}
	s.cache.lock.RUnlock()

	end := offset + q.Limit
	if q.Limit != 0 && end < opts.Limit {
		opts.Limit = end
	}

	// Number of items iterator returned
	count := 0
	// Object names
	names := []string{}
	doneFetching := false

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			names = []string{}
			return nil
		},
		Next: func() (dsq.Result, bool) {
			if q.Limit != 0 && count == q.Limit {
				return dsq.Result{}, false
			}

			for len(names) == 0 || offset > 0 {
				if doneFetching {
					return dsq.Result{}, false
				}

				newNames, err := s.conn.ObjectNames(s.Container, &opts)
				if err != nil {
					return dsq.Result{Error: err}, false
				}

				newLen := len(newNames)
				if newLen < opts.Limit {
					doneFetching = true
				}
				if newLen == 0 && len(names) == 0 {
					return dsq.Result{}, false
				}

				opts.Marker = newNames[newLen-1]

				if offset > 0 {
					if offset < newLen {
						newNames = newNames[offset:]
						offset = 0
					} else {
						newNames = []string{}
						offset -= newLen
					}
				}

				names = append(names, newNames...)
			}

			count++

			name := names[0]
			names = names[1:]

			if len(names) == 0 && q.Limit > count && (q.Limit-count) < opts.Limit {
				opts.Limit = q.Limit - count
			}

			// Cache the last item
			if doneFetching || (q.Limit > 0 && count == q.Limit) {
				s.cache.lock.Lock()
				s.cache.prefix = opts.Prefix
				s.cache.index = q.Offset + count
				s.cache.name = name
				s.cache.lock.Unlock()
			}

			key := "/" + name

			if q.KeysOnly {
				return dsq.Result{Entry: dsq.Entry{Key: key}}, true
			}

			b, err := s.conn.ObjectGetBytes(s.Container, name)
			if err != nil {
				return dsq.Result{Error: err}, false
			}
			return dsq.Result{Entry: dsq.Entry{Key: key, Value: b}}, true
		},
	}), nil
}

func (s *SwiftContainer) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

func (s *SwiftContainer) Close() error {
	return nil
}

func (s *SwiftContainer) DiskUsage() (uint64, error) {
	c, _, err := s.conn.Container(s.Container)
	if err != nil {
		return 0, err
	}
	return uint64(c.Bytes), nil
}

func (s *SwiftContainer) Batch(ctx context.Context) (ds.Batch, error) {
	return &swiftBatch{
		s:         s,
		putData:   nil,
		tarWriter: nil,
		delKeys:   nil,
	}, nil
}

type swiftBatch struct {
	s         *SwiftContainer
	tarWriter *tar.Writer
	putData   *bytes.Buffer
	delKeys   []string
}

func (b *swiftBatch) Put(ctx context.Context, k ds.Key, val []byte) error {
	if b.tarWriter == nil {
		b.putData = new(bytes.Buffer)
		b.tarWriter = tar.NewWriter(b.putData)
	}
	header := tar.Header{
		Typeflag: tar.TypeReg,
		Name:     k.String(),
		Size:     int64(len(val)),
	}

	if err := b.tarWriter.WriteHeader(&header); err != nil {
		return err
	}
	if _, err := b.tarWriter.Write(val); err != nil {
		return err
	}
	return nil
}

func (b *swiftBatch) Delete(ctx context.Context, k ds.Key) error {
	b.delKeys = append(b.delKeys, keyToName(k))
	return nil
}

func (b *swiftBatch) Commit(ctx context.Context) error {
	b.s.cache.Invalidate()

	if b.tarWriter != nil {
		if err := b.tarWriter.Close(); err != nil {
			return err
		}

		_, err := b.s.BulkUpload(b.s.Container, b.putData, swift.UploadTar, nil)
		if err != nil {
			return err
		}
	}

	if len(b.delKeys) > 0 {
		if _, err := b.s.BulkDelete(b.s.Container, b.delKeys); err != nil {
			return err
		}
	}

	return nil
}

var _ ds.Batching = (*SwiftContainer)(nil)
