package vector

import (
	"bytes"
	"fmt"
	"math"

	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/directory"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
)

/*
 * Vector stores each of its values using its index as the key.
 * The size of a vector is equal to the index of its last key + 1.
 *
 * For indexes smaller than the vector's size that have no associated key
 * in the database, the value will be the specified defaultValue.
 *
 * If the last value in the vector has the default value, its key will
 * always be set so that size can be determined.
 *
 * By creating Vector with a Subspace, all kv pairs modified by the
 * layer will have keys that start within that Subspace.
 */

type Vector struct {
	subspace     directory.DirectorySubspace
	defaultValue string
}

/*
 * VectRange - A structure for holding vector range parameters
 */
type VectRange struct {
	Start int64
	Stop  int64
	Step  int64
}

/*****************************************************************************
 * Public Methods
 ****************************************************************************/

// Get the number of items in the Vector. This number includes the sparsely represented items.
func (vect *Vector) Size(tr fdb.Transaction) (int64, error) {

	begin, end := vect.subspace.FDBRangeKeys()

	// GET is a blocking operation
	lastkey, err := tr.GetKey(fdb.LastLessOrEqual(end)).Get()
	if err != nil {
		return 0, err
	}
	// lastkey < beginKey indicates an empty vector
	if bytes.Compare(lastkey, begin.FDBKey()) == -1 {
		return 0, nil
	}

	index, err := vect.indexAt(lastkey)
	if err != nil {
		return 0, err
	}

	return index + 1, nil
}

// Set the value at a particular index in the Vector.
func (vect *Vector) Set(index int64, val interface{}, tr fdb.Transaction) error {
	v, err := ValPack(val)
	if err != nil {
		return err
	}
	tr.Set(vect.keyAt(index), v)
	return nil
}

// Get the item at the specified index.
func (vect *Vector) Get(index int64, tr fdb.Transaction) (*Value, error) {
	if index < 0 {
		return nil, fmt.Errorf("vector.get: index '%d' out of range", index)
	}

	// Instead of getting key directly we want to ensure key is within vector
	// subspace and if it is even if no key exists, provide a sparse default value.
	// If key is not within vector extents, then we throw an out-of-range error.
	start := vect.keyAt(index)
	_, end := vect.subspace.FDBRangeKeys()
	keyRange := fdb.KeyRange{
		Begin: start,
		End:   end,
	}
	ropts := fdb.RangeOptions{Limit: 1}

	justOne, err := tr.GetRange(keyRange, ropts).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(justOne) == 0 {
		return nil, fmt.Errorf("vector.get: index '%d' out of range", index)
	}
	// if this is a direct hit we return the value at the key index.
	if bytes.Compare(start, justOne[0].Key) == 0 {
		v, err := ValUnpack(justOne[0].Value)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	// If it is not, we fullfill sparsity and return the default Value.
	return &Value{}, nil
}

// Push a single item onto the end of the Vector.
func (vect *Vector) Push(val interface{}, tr fdb.Transaction) error {
	size, err := vect.Size(tr)
	if err != nil {
		return err
	}

	v, err := ValPack(val)
	if err != nil {
		return err
	}

	tr.Set(vect.keyAt(size), v)

	return nil
}

// Get and pops the last item off the Vector.
func (vect *Vector) Pop(tr fdb.Transaction) (*Value, error) {

	// Read the last two entries so we can check if the second to last item
	// is being represented sparsely. If so, we will be required to set it
	// to the default value
	ropts := fdb.RangeOptions{
		Limit:   2,
		Reverse: true,
	}
	lastTwo, err := tr.GetRange(vect.subspace, ropts).GetSliceWithError()
	if err != nil {
		return nil, err
	}

	indices := make([]int64, 2)
	for i := 0; i < len(lastTwo); i++ {
		index, err := vect.indexAt(lastTwo[i].Key)
		if err != nil {
			return nil, err
		}
		indices[i] = index
	}

	// Vector was empty // Should this be an error?
	if len(lastTwo) == 0 {
		return &Value{}, nil

	} else if indices[0] == 0 {
		// pass
	} else if len(lastTwo) == 1 || indices[0] > indices[1]+1 {
		// Second to last item is being represented sparsely
		v, err := ValPack(vect.defaultValue) //
		if err != nil {
			return nil, err
		}
		tr.Set(vect.keyAt(indices[0]-1), v)
	}

	tr.Clear(lastTwo[0].Key)

	val, err := ValUnpack(lastTwo[0].Value)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Get the value of the last item in the Vector.
func (vect *Vector) Back(tr fdb.Transaction) (*Value, error) {
	ropts := fdb.RangeOptions{
		Limit:   1,
		Reverse: true,
	}
	last, err := tr.GetRange(vect.subspace, ropts).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(last) == 0 {
		// should this be an error?
		return &Value{}, nil
	}

	val, err := ValUnpack(last[0].Value)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Get the value of the first item in the Vector.
func (vect *Vector) Front(tr fdb.Transaction) (*Value, error) {
	return vect.Get(0, tr)
}

// Get a range of items in the Vector, returned as a generator.
// To get the range to the last value, set endIdx as -1.
// Empty VectRange (or setting all values to 0) will return the
// full range.
func (vect *Vector) GetRange(vro VectRange, tr fdb.Transaction) (*Vectorator, error) {
	size, err := vect.Size(tr)
	if err != nil {
		return nil, err
	}

	if vro.Stop == 0 {
		vro.Stop = size
	} else if vro.Stop < 0 {
		vro.Stop = int64(math.Max(0.0, float64(size+vro.Stop)))
	}

	if vro.Start < 0 {
		vro.Start = int64(math.Max(0.0, float64(size+vro.Start)))
	}

	if vro.Step == 0 {
		// step has not been set
		if vro.Start <= vro.Stop {
			vro.Step = 1
		} else {
			vro.Step = -1
		}
	}

	kr := fdb.KeyRange{}

	if vro.Step > 0 {
		kr.Begin = vect.keyAt(vro.Start)
		kr.End = vect.keyAt(vro.Stop)
	} else {
		kr.End = vect.keyAt(vro.Start + 1)
		kr.Begin = vect.keyAt(vro.Stop + 1)
	}

	rr := tr.GetRange(kr, fdb.RangeOptions{Reverse: vro.Step < 0})

	return &Vectorator{rr.Iterator(), vect}, nil

}

// Remove all items from the Vector.
func (vect *Vector) Clear(tr fdb.Transaction) {
	tr.ClearRange(vect.subspace)
}

/*****************************************************************************
 * Private Methods
 ****************************************************************************/

// Get the subspace key for a given index
func (vect *Vector) keyAt(index int64) fdb.Key {
	tup := tuple.Tuple{index}
	return vect.subspace.Pack(tup)
}

// Get the index for given key in subspace
func (vect *Vector) indexAt(key fdb.Key) (int64, error) {
	islice, err := vect.subspace.Unpack(key)
	if err != nil {
		return 0, err
	}
	return islice[0].(int64), nil
}
