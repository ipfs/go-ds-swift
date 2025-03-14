package swiftds

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
	"/g":     "",
}

// returns datastore, and a function to call on exit.
// (this garbage collects). So:
//
//	d, close := newDS(t)
//	defer close()
func newDS(t *testing.T) (*SwiftContainer, func()) {
	conf := Config{}

	conf.AuthUrl = os.Getenv("SDS_URL")
	conf.TenantId = os.Getenv("SDS_TENANT_ID")
	conf.Tenant = os.Getenv("SDS_TENANT_NAME")

	av, _ := strconv.ParseInt(os.Getenv("SDS_ID_API_VER"), 10, 32)
	conf.AuthVersion = int(av)

	conf.UserName = os.Getenv("SDS_USER")
	conf.ApiKey = os.Getenv("SDS_PASS")

	conf.Region = os.Getenv("SDS_REGION")

	conf.Container = "swiftds_test"

	d, err := NewSwiftDatastore(conf)
	if err != nil {
		t.Fatal(err)
	}

	objs, err := d.Connection.Objects(conf.Container, nil)
	if err != nil {
		t.Fatal(err)
	}

	sobjs := make([]string, len(objs))
	for i, o := range objs {
		sobjs[i] = o.Name
	}

	_, err = d.Connection.BulkDelete(conf.Container, sobjs)
	if err != nil {
		t.Fatal(err)
	}

	return d, func() {
		d.Close()
	}
}

func addTestCases(t *testing.T, d *SwiftContainer, testcases map[string]string) {
	ctx := context.Background()

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		if err := d.Put(ctx, dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(ctx, dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
		size, err := d.GetSize(ctx, dsk)
		if size != len(v) {
			t.Errorf("%s size differ: %d != %d", k, len(v), size)
		}
	}
}
func TestQuery(t *testing.T) {
	ctx := context.Background()

	d, done := newDS(t)
	defer done()

	addTestCases(t, d, testcases)

	rs, err := d.Query(ctx, dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)

	// test offset and limit

	rs, err = d.Query(ctx, dsq.Query{Prefix: "/a/", Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)

	expectCache(t, d, "/a/", "/a/c", 4)

	rs, err = d.Query(ctx, dsq.Query{Prefix: "/a/", Offset: 4, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/d",
	}, rs)

	expectCache(t, d, "/a/", "/a/d", 5)

	rs, err = d.Query(ctx, dsq.Query{Prefix: "/a/", Offset: 5, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{}, rs)

	rs, err = d.Query(ctx, dsq.Query{Prefix: "/a/", Offset: 1})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)
}

func expectCache(t *testing.T, d *SwiftContainer, prefix string, name string, index int) {
	cprefix := "/" + d.cache.prefix
	if cprefix != prefix {
		t.Errorf("wrong cache prefix: %s != %s", cprefix, prefix)
	}

	cname := "/" + d.cache.name
	if cname != name {
		t.Errorf("wrong cache name: %s != %s", cname, name)
	}

	if d.cache.index != index {
		t.Errorf("wrong cache index: %d != %d", d.cache.index, index)
	}
}

func TestHas(t *testing.T) {
	ctx := context.Background()
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(ctx, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	if !has {
		t.Error("Key should be found")
	}

	has, err = d.Has(ctx, ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}
}

func TestNotExistGet(t *testing.T) {
	ctx := context.Background()
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(ctx, ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}

	val, err := d.Get(ctx, ds.NewKey("/a/b/c/d"))
	if val != nil {
		t.Error("Key should not be found")
	}

	if err != ds.ErrNotFound {
		t.Error("Error was not set to ds.ErrNotFound")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(ctx, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if !has {
		t.Error("Key should be found")
	}

	err = d.Delete(ctx, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	has, err = d.Has(ctx, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if has {
		t.Error("Key should not be found")
	}
}

func TestGetEmpty(t *testing.T) {
	ctx := context.Background()
	d, done := newDS(t)
	defer done()

	err := d.Put(ctx, ds.NewKey("/a"), []byte{})
	if err != nil {
		t.Error(err)
	}

	v, err := d.Get(ctx, ds.NewKey("/a"))
	if err != nil {
		t.Error(err)
	}

	if len(v) != 0 {
		t.Error("expected 0 len []byte form get")
	}
}

func expectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func TestBatching(t *testing.T) {
	d, done := newDS(t)
	defer done()

	ctx := context.Background()
	b, err := d.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(ctx, ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(ctx, ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}

	//Test delete

	b, err = d.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(ctx, ds.NewKey("/a/b"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(ctx, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := d.Query(ctx, dsq.Query{Prefix: "/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a",
		"/a/b/d",
		"/a/c",
		"/a/d",
		"/e",
		"/f",
		"/g",
	}, rs)

}

// Tests from basic_tests from go-datastore

func TestBasicPutGet(t *testing.T) {
	ctx := context.Background()
	d, done := newDS(t)
	defer done()

	k := ds.NewKey("foo")
	val := []byte("Hello Datastore!")

	err := d.Put(ctx, k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := d.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	out, err := d.Get(ctx, k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("value received on get wasnt what we expected:", out)
	}

	have, err = d.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	err = d.Delete(ctx, k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = d.Has(ctx, k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}
}

func SubtestNotFounds(t *testing.T) {
	ctx := context.Background()
	d, done := newDS(t)
	defer done()

	badk := ds.NewKey("notreal")

	val, err := d.Get(ctx, badk)
	if err != ds.ErrNotFound {
		t.Fatal("expected ErrNotFound for key that doesnt exist, got: ", err)
	}

	if val != nil {
		t.Fatal("get should always return nil for not found values")
	}

	have, err := d.Has(ctx, badk)
	if err != nil {
		t.Fatal("error calling has on not found key: ", err)
	}
	if have {
		t.Fatal("has returned true for key we don't have")
	}
}

func TestManyKeysAndQuery(t *testing.T) {
	ctx := context.Background()
	d, done := newDS(t)
	defer done()

	var keys []ds.Key
	var keystrs []string
	var values [][]byte
	count := 100
	for i := 0; i < count; i++ {
		s := fmt.Sprintf("%dkey%d", i, i)
		dsk := ds.NewKey(s)
		keystrs = append(keystrs, dsk.String())
		keys = append(keys, dsk)
		buf := make([]byte, 64)
		rand.Read(buf)
		values = append(values, buf)
	}

	t.Logf("putting %d values", count)
	for i, k := range keys {
		err := d.Put(ctx, k, values[i])
		if err != nil {
			t.Fatalf("error on put[%d]: %s", i, err)
		}
	}

	t.Log("getting values back")
	for i, k := range keys {
		val, err := d.Get(ctx, k)
		if err != nil {
			t.Fatalf("error on get[%d]: %s", i, err)
		}

		if !bytes.Equal(val, values[i]) {
			t.Fatal("input value didnt match the one returned from Get")
		}
	}

	t.Log("querying values")
	q := dsq.Query{KeysOnly: true}
	resp, err := d.Query(ctx, q)
	if err != nil {
		t.Fatal("calling query: ", err)
	}

	t.Log("aggregating query results")
	var outkeys []string
	for {
		res, ok := resp.NextSync()
		if res.Error != nil {
			t.Fatal("query result error: ", res.Error)
		}
		if !ok {
			break
		}

		outkeys = append(outkeys, res.Key)
	}

	t.Log("verifying query output")
	sort.Strings(keystrs)
	sort.Strings(outkeys)

	if len(keystrs) != len(outkeys) {
		t.Fatalf("got wrong number of keys back, %d != %d", len(keystrs), len(outkeys))
	}

	for i, s := range keystrs {
		if outkeys[i] != s {
			t.Fatalf("in key output, got %s but expected %s", outkeys[i], s)
		}
	}

	t.Log("deleting all keys")
	for _, k := range keys {
		if err := d.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
}
