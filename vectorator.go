package vector

import "github.com/FoundationDB/fdb-go/fdb"

/*
 * Vecterator - a wrapper around the default rangeIterator that
 * returns VKeyVal's instead of KeyValue's (it unboxes the []byte value
 * and unpacks the key into an index.
 */
type Vectorator struct {
	ri   *fdb.RangeIterator
	vect *Vector
}

func (vi *Vectorator) Advance() bool {
	return vi.ri.Advance()
}

func (vi *Vectorator) Get() (iv IndexValue, err error) {

	kv, err := vi.ri.Get()
	if err != nil {
		return
	}

	val, err := ValUnpack(kv.Value)
	if err != nil {
		return
	}

	idx, err := vi.vect.indexAt(kv.Key)
	if err != nil {
		return
	}

	iv = IndexValue{
		Index: idx,
		Value: val,
	}

	return
}
