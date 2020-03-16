package lz4msgpack_test

import (
	"reflect"
	"testing"

	"github.com/shamaton/lz4msgpack"
	"github.com/shamaton/msgpack"
)

type marshaller func(v interface{}) ([]byte, error)
type unMarshaller func([]byte, interface{}) error

func Test(t *testing.T) {
	type Data struct {
		A int
		B float32
		C []string
	}

	data := Data{
		A: 4578234323,
		B: 1.46437485,
		C: []string{"Hello World", "Hello World", "Hello World", "Hello World", "Hello World"},
	}
	t.Log(data)

	tester := func(name string, m marshaller, u unMarshaller) {
		b, err := m(&data)
		if err != nil {
			t.Fatal("marshal failed", err)
		}
		t.Logf("%s: %d", name, len(b))
		var data1 Data
		err = u(b, &data1)
		if err != nil {
			t.Fatal("unmarshal failed", err)
		}
		if !reflect.DeepEqual(data, data1) {
			t.Fatal(name + " Error")
		}
	}

	tester("          msgpack.Marshal", msgpack.Encode, msgpack.Decode)
	tester("   msgpack.MarshalAsArray", msgpack.EncodeStructAsArray, msgpack.DecodeStructAsArray)
	tester("       lz4msgpack.Marshal", lz4msgpack.Marshal, lz4msgpack.Unmarshal)
	tester("lz4msgpack.MarshalAsArray", lz4msgpack.MarshalAsArray, lz4msgpack.UnmarshalAsArray)
}
