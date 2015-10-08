// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBufferSequence(t *testing.T) {
	seq := newSequence()

	seq.set(100)
	require.Equal(t, int64(100), seq.get())

	seq.set(20000)
	require.Equal(t, int64(20000), seq.get())
}

func TestBufferReadFrom(t *testing.T) {
	testFillBuffer(t, 144, 16384)
	testFillBuffer(t, 2048, 16384)
	testFillBuffer(t, 3072, 16384)
}

func TestBufferReadBytes(t *testing.T) {
	buf := testFillBuffer(t, 2048, 16384)

	testReadBytes(t, buf)
}

func TestBufferCommitBytes(t *testing.T) {
	buf := testFillBuffer(t, 2048, 16384)

	testCommit(t, buf)
}

func TestBufferConsumerProducerRead(t *testing.T) {
	buf, err := newBuffer(16384)

	require.NoError(t, err)

	testRead(t, buf)
}

func TestBufferConsumerProducerWriteTo(t *testing.T) {
	buf, err := newBuffer(16384)

	require.NoError(t, err)

	testWriteTo(t, buf)
}

func TestBufferConsumerProducerPeekCommit(t *testing.T) {
	buf, err := newBuffer(16384)

	require.NoError(t, err)

	testPeekCommit(t, buf)
}

func TestBufferPeek(t *testing.T) {
	buf := testFillBuffer(t, 2048, 16384)

	peekBuffer(t, buf, 100)
	peekBuffer(t, buf, 1000)
}

func BenchmarkBufferConsumerProducerRead(b *testing.B) {
	buf, _ := newBuffer(0)
	benchmarkRead(b, buf)
}

func testFillBuffer(t *testing.T, bufsize, ringsize int64) *buffer {
	buf, err := newBuffer(ringsize)

	require.NoError(t, err)

	fillBuffer(t, buf, bufsize)

	require.Equal(t, int(bufsize), buf.Len())

	return buf
}

func fillBuffer(t *testing.T, buf *buffer, bufsize int64) {
	p := make([]byte, bufsize)
	for i := range p {
		p[i] = 'a'
	}

	n, err := buf.ReadFrom(bytes.NewBuffer(p))

	require.Equal(t, bufsize, n)
	require.Equal(t, err, io.EOF)
}

func peekBuffer(t *testing.T, buf *buffer, n int) {
	pkbuf, err := buf.ReadPeek(n)

	require.NoError(t, err)
	require.Equal(t, n, len(pkbuf))

	for _, b := range pkbuf {
		require.Equal(t, byte('a'), b)
	}
}

func testPeekCommit(t *testing.T, buf *buffer) {
	n := 20000

	go func(n int64) {
		fillBuffer(t, buf, n)
	}(int64(n))

	i := 0

	for n > 0 {
		pkbuf, _ := buf.ReadPeek(1024)
		l, err := buf.ReadCommit(len(pkbuf))

		require.NoError(t, err)

		n -= l
		i += l
	}
}

func testWriteTo(t *testing.T, buf *buffer) {
	n := int64(20000)

	go func(n int64) {
		fillBuffer(t, buf, n)
		time.Sleep(time.Millisecond * 100)
		buf.Close()
	}(n)

	m, err := buf.WriteTo(bytes.NewBuffer(make([]byte, n)))

	require.Equal(t, io.EOF, err)
	require.Equal(t, int64(20000), m)
}

func testRead(t *testing.T, buf *buffer) {
	n := int64(20000)

	go func(n int64) {
		fillBuffer(t, buf, n)
	}(n)

	p := make([]byte, n)
	i := 0

	for n > 0 {
		l, err := buf.Read(p[i:])

		require.NoError(t, err)

		n -= int64(l)
		i += l
	}
}

func testCommit(t *testing.T, buf *buffer) {
	n, err := buf.ReadCommit(256)

	require.NoError(t, err)
	require.Equal(t, 256, n)

	_, err = buf.ReadCommit(2048)

	require.Equal(t, ErrBufferInsufficientData, err)
}

func testReadBytes(t *testing.T, buf *buffer) {
	p := make([]byte, 256)
	n, err := buf.Read(p)

	require.NoError(t, err)
	require.Equal(t, 256, n)

	p2 := make([]byte, 4096)
	n, err = buf.Read(p2)

	require.NoError(t, err)
	require.Equal(t, 2048-256, n)
}

func benchmarkRead(b *testing.B, buf *buffer) {
	n := int64(b.N)

	go func(n int64) {
		p := make([]byte, n)
		buf.ReadFrom(bytes.NewBuffer(p))
	}(n)

	p := make([]byte, n)
	i := 0

	for n > 0 {
		l, _ := buf.Read(p[i:])

		n -= int64(l)
		i += l
	}
}
