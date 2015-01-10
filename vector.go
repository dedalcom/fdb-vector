package vector

import (
	"bytes"

	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/directory"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
)

type Vector struct {
	subspace     directory.DirectorySubspace
	defaultValue string
}

func (vect *Vector) Set(index int64, val string, tr fdb.Transaction) {
	tr.Set(vect.keyAt(index), vect.valpack(val))
}

func (vect *Vector) Push(val string, tr fdb.Transaction) error {
	size = vect.Size(tr)
	if err != nil {
		return error
	}

	tr.Set(vect.keyAt(size), vect.valpack(val))

	return nil
}

func (vect *Vector) Pop(tr fdb.Transaction) (string, error) {

	// Read the last two entries so we can check if the second to last item
	// is being represented sparsely. If so, we will be required to set it
	// to the default value
	ropts := fdb.RangeOptions{
		Limit:   2,
		Reverse: true,
	}
	lastTwo := tr.GetRange(vect.subspace, ropts).GetSliceOrPanic()

	indices := make([]int64, 2)
	for i := 0; i < len(lastTwo); i++ {
		index, err := vect.key2index(lastTwo[i].Key)
		if err != nil {
			return vect.defaultValue, err
		}
		indices[i] = index
	}

	// Vector was empty
	if len(lastTwo) == 0 {
		return vect.defaultValue, nil

	} else if indices[0] == 0 {

	} else if len(lastTwo) == 1 || indices[0] > indices[1]+1 {
		// Second to last item is being represented sparsely
		tr.Set(vect.keyAt(indices[0]-1), vect.valpack(vect.defaultValue))
	}

	tr.Clear(lastTwo[0].Key)
	valslice, err := tuple.Unpack(lastTwo[0].Value)

	if err != nil {
		return vect.defaultValue, err
	}

	return valslice[0].(string), nil
}

/*
 * Private Methods
 */
// size get number of keys
// b<locking
func (vect *Vector) Size(tr fdb.Transaction) (int64, error) {

	begin, end := vect.subspace.FDBRangeKeys()

	lastkey, err := tr.GetKey(fdb.LastLessOrEqual(end)).Get()
	if err != nil {
		return 0, err
	}

	if bytes.Compare(lastkey, begin.FDBKey()) == -1 {
		return 0
	}

	index, err := vect.key2index(lastkey)
	if err != nil {
		return 0, err
	}

	return index + 1, nil
}

func (vect *Vector) keyAt(index int64) fdb.Key {
	tup := tuple.Tuple{index}
	return vect.subspace.Pack(tup)
}

func (vect *Vector) key2index(key fdb.Key) (int64, error) {
	islice, err := vect.subspace.Unpack(key)
	if err != nil {
		return 0, err
	}
	return islice[0].(int64), nil
}

func (vect *Vector) clear(tr fdb.Transaction) {
	tr.ClearRange(vect.subspace)
}

func (vect *Vector) valpack(val string) []byte {
	t := tuple.Tuple{val}
	return t.Pack()
}
