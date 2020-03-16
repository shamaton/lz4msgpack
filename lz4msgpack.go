package lz4msgpack

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pierrec/lz4"
	"github.com/shamaton/msgpack"
)

const (
	codeExt8  = 0xc7
	codeExt16 = 0xc8
	codeExt32 = 0xc9

	codeInt32 = 0xd2

	extTypeLz4           = 99
	decompressDataLength = 4
	maxPrefixLength      = 11
)

type unMarshaller func([]byte, interface{}) error

// Marshal returns bytes that is the MessagePack encoded and lz4 compressed.
func Marshal(v interface{}) ([]byte, error) {
	return marshal(v, msgpack.Encode)
}

// MarshalAsArray returns bytes as array format that is the MessagePack encoded and lz4 compressed.
func MarshalAsArray(v interface{}) ([]byte, error) {
	return marshal(v, msgpack.EncodeStructAsArray)
}

func marshal(v interface{}, marshaller func(interface{}) ([]byte, error)) ([]byte, error) {
	data, err := marshaller(v)
	if err != nil {
		return data, err
	}
	return compress(data)
}

// compress by lz4
func compress(data []byte) ([]byte, error) {

	buf := make([]byte, maxPrefixLength+lz4.CompressBlockBound(len(data)))
	size, err := lz4.CompressBlockHC(data, buf[maxPrefixLength:], 0)
	if err != nil || size == 0 {
		return data, err
	}

	dataSize := size + 1 + decompressDataLength
	startIndex := maxPrefixLength - 1 - decompressDataLength
	index := -1

	switch {
	case dataSize <= math.MaxUint8:

		startIndex -= 3
		index = startIndex

		buf[index] = codeExt8
		index++

		buf[index] = byte(dataSize)
		index++

	case dataSize <= math.MaxUint16:

		startIndex -= 4
		index = startIndex
		buf[index] = codeExt16
		index++

		binary.BigEndian.PutUint16(buf[index:index+2], (uint16)(dataSize))
		index += 2

	case dataSize <= math.MaxUint32:
		startIndex -= 6
		index = startIndex

		buf[index] = codeExt32
		index++

		binary.BigEndian.PutUint32(buf[index:index+4], (uint32)(dataSize))
		index += 4

	}

	if len(data) <= size+maxPrefixLength-startIndex {
		return data, err
	}

	buf[index] = extTypeLz4
	index++
	buf[index] = codeInt32
	index++
	binary.BigEndian.PutUint32(buf[index:index+decompressDataLength], (uint32)(len(data)))
	return buf[startIndex : maxPrefixLength+size], err
}

// Unmarshal decodes the MessagePack-encoded data and stores the result
// in the value pointed to by v.
// In case of data compressed by lz4, it will be uncompressed before decode.
func Unmarshal(data []byte, v interface{}) error {
	return unmarshal(data, v, msgpack.Decode)
}

// UnmarshalAsArray is mostly same functional.
// But struct data's format must be array format.
func UnmarshalAsArray(data []byte, v interface{}) error {
	return unmarshal(data, v, msgpack.DecodeStructAsArray)
}

func unmarshal(data []byte, v interface{}, f unMarshaller) error {

	code := data[0]

	index := 0
	switch code {
	case codeExt8:
		index = 2

	case codeExt16:
		index = 3

	case codeExt32:
		index = 5

	default:
		return f(data, v)
	}
	if data[index] != extTypeLz4 {
		return fmt.Errorf("not ext type lz4 %v", data[index])
	}
	index++

	if data[index] != codeInt32 {
		return fmt.Errorf("not code int32 %v", data[index])
	}
	index++

	buf := make([]byte, binary.BigEndian.Uint32(data[index:index+decompressDataLength]))
	index += decompressDataLength

	_, err := lz4.UncompressBlock(data[index:], buf)
	if err != nil {
		return err
	}
	return f(buf, v)
}
