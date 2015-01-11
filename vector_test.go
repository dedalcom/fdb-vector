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
			return nil, fmt.Errorf("Expected popped value to be size %s, got %d instead", vector.defaultValue, v)
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

func TestPackUnpack(t *testing.T) {

	db := fdb.MustOpenDefault()
	subspace, err := directory.CreateOrOpen(db, []string{"tests", "vector"}, []byte{0})

	if err != nil {
		panic(err)
	}

	vector := Vector{subspace: subspace}

	b, err := vector.valPack("")
	if err != nil {
		t.Error("valPack fails packing empty string")
	}
	v, err := vector.valUnpack(b)
	if err != nil {
		t.Error("valPack fails unpacking", err)
	}
	if !v.IsString || v.String != "" {
		t.Error("valPack fails unpacking empty string. Instead got", v.String)
	}

	b, err = vector.valPack("☢ € → ☎ ❄mung")
	if err != nil {
		t.Error("valPack fails packing string '☢ € → ☎ ❄mung'")
	}
	v, err = vector.valUnpack(b)
	if err != nil {
		t.Error("valPack fails unpacking", err)
	}
	if !v.IsString || v.String != "☢ € → ☎ ❄mung" {
		t.Error("valPack fails unpacking string '☢ € → ☎ ❄mung'. Instead got", v.String)
	}

	b, err = vector.valPack(3.25)
	if err != nil {
		t.Error("valPack fails packing 3.25")
	}
	v, err = vector.valUnpack(b)
	if err != nil {
		t.Error("valPack fails unpacking", err)
	}
	if !v.IsFloat || v.Float != 3.25 {
		t.Error("valPack fails unpacking 3.25. Instead got", v.Float)
	}

	b, err = vector.valPack(vector)
	if err == nil {
		t.Error("expected error for unsupported pack type. Instead got none")
	}
}
