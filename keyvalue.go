package vector

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type IndexValue struct {
	Index int64
	Value *Value
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

// Pack Value supported values into a Value byte array
func ValPack(val interface{}) ([]byte, error) {

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
func ValUnpack(b []byte) (*Value, error) {

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
