package swiftds

import (
	"fmt"

	swift "github.com/ncw/swift"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

type SwiftContainer struct {
	conn *swift.Connection

	Config
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
		conn: c,

		Config: conf,
	}, nil
}

func (s *SwiftContainer) Get(k ds.Key) ([]byte, error) {
	data, err := s.conn.ObjectGetBytes(s.Container, k.String())
	switch err {
	case nil:
		return data, nil
	case swift.ObjectNotFound:
		return nil, ds.ErrNotFound
	default:
		return nil, err
	}
}

func (s *SwiftContainer) Delete(k ds.Key) error {
	return s.conn.ObjectDelete(s.Container, k.String())
}

func (s *SwiftContainer) Put(k ds.Key, val []byte) error {
	return s.conn.ObjectPutBytes(s.Container, k.String(), val, "application/octet-stream")
}

func (s *SwiftContainer) Has(k ds.Key) (bool, error) {
	_, _, err := s.conn.Object(s.Container, k.String())
	switch err {
	case nil:
		return true, nil
	case swift.ObjectNotFound:
		return false, nil
	default:
		return false, err
	}
}

func (s *SwiftContainer) GetSize(k ds.Key) (int, error) {
	return 0, nil
}

func (s *SwiftContainer) Query(q dsq.Query) (dsq.Results, error) {
	opts := swift.ObjectsOpts{
		Prefix: q.Prefix,
		Limit:  q.Limit + q.Offset,
	}

	objs, err := s.conn.Objects(s.Container, &opts)
	if err != nil {
		return nil, err
	}

	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("swiftds doesnt support filters or orders")
	}

	res := make([]dsq.Entry, len(objs[q.Offset:]))
	for i, obj := range objs[q.Offset:] {
		res[i] = dsq.Entry{Key: obj.Name}
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			objs = []swift.Object{}
			return nil
		},
		Next: func() (dsq.Result, bool) {
			if len(res) == 0 {
				return dsq.Result{}, false
			}

			obj := res[0]
			res = res[1:]

			if q.KeysOnly {
				return dsq.Result{Entry: obj}, true
			}

			b, err := s.conn.ObjectGetBytes(s.Container, obj.Key)
			if err != nil {
				return dsq.Result{Error: err}, false
			}
			return dsq.Result{Entry: dsq.Entry{Key: obj.Key, Value: b}}, true
		},
	}), nil
}

func (s *SwiftContainer) Sync(prefix ds.Key) error {
	return nil
}

func (s *SwiftContainer) Close() error {
	return nil
}

func (s *SwiftContainer) Batch() (ds.Batch, error) {
	return &swiftBatch{s}, nil
}

type swiftBatch struct {
	s *SwiftContainer
}

func (b *swiftBatch) Put(k ds.Key, val []byte) error {
	return b.s.Put(k, val)
}

func (b *swiftBatch) Delete(k ds.Key) error {
	return b.s.Delete(k)
}

func (b *swiftBatch) Commit() error {
	// TODO: can optimize using bulkUpload/bulkDelete
	return nil
}

var _ ds.Batching = (*SwiftContainer)(nil)
