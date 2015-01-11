package vector

import (
	"bytes"
	"encoding/binary"
	"fmt"

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
 * Value is the return value from unpacking an element of a Vector.
 * As type information is serialized along with a value during packing
 * this information is available when the value is unserialized during unpacking.
 * It is stored inside a Value type with helper is[type] bool fields.
 */
type Value struct {
	IsFloat  bool
	IsInt    bool
	IsString bool
	Float    float64
	Int      int64
	String   string
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
	v, err := vect.valPack(val)
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
		v, err := vect.valUnpack(justOne[0].Value)
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

	v, err := vect.valPack(val)
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
		v, err := vect.valPack(vect.defaultValue) //
		if err != nil {
			return nil, err
		}
		tr.Set(vect.keyAt(indices[0]-1), v)
	}

	tr.Clear(lastTwo[0].Key)

	val, err := vect.valUnpack(lastTwo[0].Value)
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

	val, err := vect.valUnpack(last[0].Value)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Get the value of the first item in the Vector.
// func (vect *Vector) Front(tr fdb.Transaction) (Value, error) {
// }

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

// Pack Value supported values into a Value byte array
func (vect *Vector) valPack(val interface{}) ([]byte, error) {

	buf := new(bytes.Buffer)

	var err error

	switch v := val.(type) {
	case int64:
		buf.WriteByte(0x01)
		err = binary.Write(buf, binary.BigEndian, v)
	case int:
		buf.WriteByte(0x01)
		err = binary.Write(buf, binary.BigEndian, int64(v))
	case float64:
		buf.WriteByte(0x02)
		err = binary.Write(buf, binary.BigEndian, v)
	case float32:
		buf.WriteByte(0x02)
		err = binary.Write(buf, binary.BigEndian, float64(v))
	case string:
		buf.WriteByte(0x03)
		_, err = buf.WriteString(v)
	default:
		err = fmt.Errorf("fdb-vector unencodable element (%v, type %T)", v, v)
	}

	return buf.Bytes(), err
}

// Unpack values into a Value structure
func (vect *Vector) valUnpack(b []byte) (*Value, error) {

	v := &Value{}

	if len(b) == 0 {
		return v, fmt.Errorf("No Byte array to Decode")
	}

	var err error
	code := b[0]
	buf := bytes.NewBuffer(b[1:])

	switch {
	case code == 0x01:
		v.IsInt = true
		err = binary.Read(buf, binary.BigEndian, &v.Int)
	case code == 0x02:
		v.IsFloat = true
		err = binary.Read(buf, binary.BigEndian, &v.Float)
	case code == 0x03:
		v.IsString = true
		v.String = string(b[1:])
	default:
		err = fmt.Errorf("unable to decode tuple element with unknown typecode %02x", code)
	}

	return v, err
}
