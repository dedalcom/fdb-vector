package vector

import (
	"fmt"
	"os"
	"testing"

	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/directory"
)

func TestMain(m *testing.M) {
	fdb.MustAPIVersion(200)
	os.Exit(m.Run())
}

func isEmpty(v *Value) bool {
	return !v.IsFloat && !v.IsInt && !v.IsString
}

func TestClear(t *testing.T) {

	db := fdb.MustOpenDefault()

	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})
	if err != nil {
		panic(err)
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		vector := Vector{subspace: subspace}

		vector.Clear(tr)

		vector.Set(0, "a", tr)
		vector.Set(1, "b", tr)

		i, err := vector.Size(tr)
		if i != 2 {
			return nil, fmt.Errorf("Expected vector to be size 1, got %d instead", i)
		}
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}

		vector.Clear(tr)

		i, err = vector.Size(tr)
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}

		if i != 0 {
			return nil, fmt.Errorf("Expected empty vector to be size 0, got %d instead", i)
		}

		return nil, nil

	})

	if e != nil {
		t.Error(e)
	}

}

func TestSize(t *testing.T) {

	db := fdb.MustOpenDefault()

	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})
	if err != nil {
		panic(err)
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		vector := Vector{subspace: subspace}
		vector.Clear(tr)

		i, err := vector.Size(tr)
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}
		if i != 0 {
			return nil, fmt.Errorf("Expected empty vector to be size 0, got %d instead", i)
		}

		vector.Set(0, "a", tr)

		i, err = vector.Size(tr)
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}
		if i != 1 {
			return nil, fmt.Errorf("Expected vector to be size 1, got %d instead", i)
		}

		return nil, nil

	})

	if e != nil {
		t.Error(e)
	}

}

func TestGetSet(t *testing.T) {

	db := fdb.MustOpenDefault()

	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})
	if err != nil {
		panic(err)
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		vector := Vector{subspace: subspace}
		vector.Clear(tr)

		err := vector.Set(3, "a", tr)
		if err != nil {
			return nil, fmt.Errorf("Set returned error: %s", err)
		}

		val, err := vector.Get(3, tr)
		if err != nil {
			return nil, fmt.Errorf("Get returned an error %s", err)
		}
		if val.String != "a" {
			return nil, fmt.Errorf("Val should be 'a' instead got: %s", val.String)
		}

		val, err = vector.Get(1, tr)
		if err != nil {
			return nil, fmt.Errorf("Get returned error: %s", err)
		}
		if !isEmpty(val) {
			return nil, fmt.Errorf("Expected empty val instead got: %s", val)
		}

		val, err = vector.Get(4, tr)
		if err == nil {
			return nil, fmt.Errorf("Expected out of range error")
		}
		if val != nil {
			return nil, fmt.Errorf("Val should be nil instead got: %s", val)
		}

		return nil, nil
	})

	if e != nil {
		t.Error(e)
	}
}

func TestPushPop(t *testing.T) {

	db := fdb.MustOpenDefault()

	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})
	if err != nil {
		panic(err)
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		vector := Vector{subspace: subspace}
		vector.Clear(tr)

		err := vector.Push("a", tr)
		if err != nil {
			return nil, fmt.Errorf("Push returned error: %s", err)
		}

		err = vector.Push("b", tr)
		if err != nil {
			return nil, fmt.Errorf("Push returned error: %s", err)
		}

		v, err := vector.Pop(tr)
		if err != nil {
			return nil, fmt.Errorf("Pop returned an error")
		}
		if v.String != "b" {
			return nil, fmt.Errorf("Expected popped value to be 'b', got %s instead", v)
		}

		v, err = vector.Pop(tr)
		if err != nil {
			return nil, fmt.Errorf("Pop returned an error")
		}
		if v.String != "a" {
			return nil, fmt.Errorf("Expected popped value to be 'a', got %s instead", v)
		}

		i, err := vector.Size(tr)
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}
		if i != 0 {
			return nil, fmt.Errorf("Expected empty vector to be size 0, got %d instead", i)
		}

		return nil, nil

	})

	if e != nil {
		t.Error(e)
	}
}

func TestSparsity(t *testing.T) {

	db := fdb.MustOpenDefault()

	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})
	if err != nil {
		panic(err)
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		vector := Vector{subspace: subspace}
		vector.Clear(tr)

		vector.Set(3, "a", tr)
		i, err := vector.Size(tr)
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}
		if i != 4 {
			return nil, fmt.Errorf("Expected vector to be size 4, got %d instead", i)
		}

		v, err := vector.Pop(tr)
		if err != nil {
			return nil, fmt.Errorf("Pop returned an error")
		}
		if v.String != "a" {
			return nil, fmt.Errorf("Expected popped value to be size 'a', got %d instead", v)
		}

		i, err = vector.Size(tr)
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}
		if i != 3 {
			return nil, fmt.Errorf("Expected vector to be size 3, got %d instead", i)
		}

		v, err = vector.Pop(tr)
		if err != nil {
			return nil, fmt.Errorf("Pop returned an error")
		}
		if v.String != vector.defaultValue {
			return nil, fmt.Errorf("Expected popped value to be %s, got %d instead", vector.defaultValue, v)
		}

		i, err = vector.Size(tr)
		if err != nil {
			return nil, fmt.Errorf("Size returned error: %s", err)
		}
		if i != 2 {
			return nil, fmt.Errorf("Expected vector to be size 2, got %d instead", i)
		}

		return nil, nil

	})

	if e != nil {
		t.Error(e)
	}
}

func TestGetRange(t *testing.T) {
	db := fdb.MustOpenDefault()
	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})
	if err != nil {
		panic(err)
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		vector := Vector{subspace: subspace}
		vector.Clear(tr)

		vals := []string{"a", "b", "c", "d", "e", "f"}

		vector.Set(0, "z", tr)
		vector.Set(1, vals[1], tr)
		vector.Set(2, vals[2], tr)
		vector.Set(3, vals[3], tr)
		vector.Set(4, vals[4], tr)
		vector.Set(5, "z", tr)

		vropts := VectRange{
			Start: 1,
			Stop:  4,
		}
		vi, err := vector.GetRange(vropts, tr)
		if err != nil {
			return nil, fmt.Errorf("vector.GetRange error: %s", err)
		}
		i := int64(1)
		for vi.Advance() {
			iv, e := vi.Get()
			if e != nil {
				return nil, fmt.Errorf("vector.GetRange iterator returned error: %s", err)
			}
			if i != iv.Index || iv.Value.String != vals[i] {
				return nil, fmt.Errorf("vector.GetRange iteration. Expected '%d %s' got '%d %s'", i, vals[i], iv.Index, iv.Value.String)
			}
			i++
		}

		return nil, nil
	})

	if e != nil {
		t.Error(e)
	}
}

func TestKeyAtIndexAt(t *testing.T) {

	db := fdb.MustOpenDefault()
	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})
	if err != nil {
		panic(err)
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		vector := Vector{subspace: subspace}
		vector.Clear(tr)

		key := vector.keyAt(3)
		i, err := vector.indexAt(key)
		if err != nil {
			return nil, fmt.Errorf("indexAt returned error: %s", err)
		}
		if i != 3 {
			return nil, fmt.Errorf("Expected key index to be 3, got %d instead", i)
		}

		return nil, nil

	})

	if e != nil {
		t.Error(e)
	}
}
