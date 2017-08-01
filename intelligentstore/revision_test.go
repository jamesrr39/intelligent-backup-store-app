package intelligentstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_areFilesTheSame(t *testing.T) {
	// test 2 simple contents
	bytesA := []byte("abc")
	fileB := bytes.NewBuffer([]byte("abc\ndef"))

	result := areFilesTheSameBytes(bytesA, fileB)
	assert.False(t, result)

	fileC := bytes.NewBuffer([]byte("abd"))

	result = areFilesTheSameBytes(bytesA, fileC)
	assert.False(t, result)

	fileD := bytes.NewBuffer([]byte("abcd"))

	result = areFilesTheSameBytes(bytesA, fileD)
	assert.False(t, result)

	// larger amounts of bytes
	controlByteBuf := bytes.NewBuffer(nil)
	for i := 0; i < 1000; i++ {

		_, err := controlByteBuf.Write([]byte("abcdefgh 123456 _+"))
		if nil != err {
			panic(err)
		}
	}

	assert.Len(t, controlByteBuf.Bytes(), 18000)

	// test two larger byte buffers are evaluated as equal
	testByteBuf1 := bytes.NewBuffer(nil)
	_, err := testByteBuf1.Write(controlByteBuf.Bytes())
	if nil != err {
		panic(err)
	}

	assert.True(t, areFilesTheSameBytes(controlByteBuf.Bytes(), testByteBuf1))

	// test when the second arg is bigger than the first
	testByteBuf2 := bytes.NewBuffer(nil)
	_, err = testByteBuf2.Write(controlByteBuf.Bytes())
	if nil != err {
		panic(err)
	}

	_, err = testByteBuf2.Write([]byte("some extra test bytes"))
	if nil != err {
		panic(err)
	}

	assert.False(t, areFilesTheSameBytes(controlByteBuf.Bytes(), testByteBuf2))

	// test when the first arg is bigger than the second
	largerControlByteBuf := bytes.NewBuffer(nil)
	_, err = largerControlByteBuf.Write(controlByteBuf.Bytes())
	if nil != err {
		panic(err)
	}

	_, err = largerControlByteBuf.Write([]byte("some extra control bytes"))
	if nil != err {
		panic(err)
	}

	testByteBuf3 := bytes.NewBuffer(nil)
	_, err = testByteBuf3.Write(controlByteBuf.Bytes())
	if nil != err {
		panic(err)
	}

	assert.False(t, areFilesTheSameBytes(largerControlByteBuf.Bytes(), testByteBuf3))

}
