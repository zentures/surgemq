// Copyright (c) 2014 Dataence, LLC. All rights reserved.
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
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/dataence/glog"
	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/sessions"
)

func getConnectMessage(conn io.Closer) (*message.ConnectMessage, error) {
	buf, err := getMessageBuffer(conn)
	if err != nil {
		glog.Debugf("Receive error: %v", err)
		return nil, err
	}

	msg := message.NewConnectMessage()

	_, err = msg.Decode(buf)
	//glog.Debugf("Received: %s", msg)
	return msg, err
}

func getConnackMessage(conn io.Closer) (*message.ConnackMessage, error) {
	buf, err := getMessageBuffer(conn)
	if err != nil {
		glog.Debugf("Receive error: %v", err)
		return nil, err
	}

	msg := message.NewConnackMessage()

	_, err = msg.Decode(buf)
	//glog.Debugf("Received: %s", msg)
	return msg, err
}

func writeMessage(conn io.Closer, msg message.Message) error {
	buf := make([]byte, msg.Len())
	_, err := msg.Encode(buf)
	if err != nil {
		glog.Debugf("Write error: %v", err)
		return err
	}
	//glog.Debugf("Writing: %s", msg)

	return writeMessageBuffer(conn, buf)
}

func getMessageBuffer(c io.Closer) ([]byte, error) {
	if c == nil {
		return nil, ErrInvalidConnectionType
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return nil, ErrInvalidConnectionType
	}

	var (
		// the message buffer
		buf []byte

		// tmp buffer to read a single byte
		b []byte = make([]byte, 1)

		// total bytes read
		l int = 0
	)

	// Let's read enough bytes to get the message header (msg type, remaining length)
	for {
		// If we have read 5 bytes and still not done, then there's a problem.
		if l > 5 {
			return nil, fmt.Errorf("connect/getMessage: 4th byte of remaining length has continuation bit set")
		}

		n, err := conn.Read(b[0:])
		if err != nil {
			glog.Debugf("Read error: %v", err)
			return nil, err
		}

		// Technically i don't think we will ever get here
		if n == 0 {
			continue
		}

		buf = append(buf, b...)
		l += n

		// Check the remlen byte (1+) to see if the continuation bit is set. If so,
		// increment cnt and continue reading. Otherwise break.
		if l > 1 && b[0] < 0x80 {
			break
		}
	}

	// Get the remaining length of the message
	remlen, _ := binary.Uvarint(buf[1:])
	buf = append(buf, make([]byte, remlen)...)

	for l < len(buf) {
		n, err := conn.Read(buf[l:])
		if err != nil {
			return nil, err
		}
		l += n
	}

	return buf, nil
}

func writeMessageBuffer(c io.Closer, b []byte) error {
	if c == nil {
		return ErrInvalidConnectionType
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return ErrInvalidConnectionType
	}

	_, err := conn.Write(b)
	return err
}

// Copied from http://golang.org/src/pkg/net/timeout_test.go
func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

func addTopic(sess sessions.Session, topic string, qos byte) error {
	topics, qoss, err := getTopicsQoss(sess)
	if err != nil {
		return err
	}

	found := false

	// Update subscription if already exist
	for i, t := range topics {
		if topic == t {
			qoss[i] = qos
			found = true
			break
		}
	}

	if !found {
		// Otherwise add it
		topics = append(topics, topic)
		qoss = append(qoss, qos)
	}

	sess.Set(keyTopics, topics)
	sess.Set(keyTopicQos, qoss)

	return nil
}

func removeTopic(sess sessions.Session, topic string) error {
	topics, qoss, err := getTopicsQoss(sess)
	if err != nil {
		return err
	}

	// Delete subscription if already exist
	for i, t := range topics {
		if topic == t {
			topics = append(topics[:i], topics[i+1:]...)
			qoss = append(qoss[:i], qoss[i+1:]...)
			break
		}
	}

	sess.Set(keyTopics, topics)
	sess.Set(keyTopicQos, qoss)

	return nil
}

func getTopicsQoss(sess sessions.Session) ([]string, []byte, error) {
	var (
		topics []string
		qoss   []byte
		ok     bool
	)

	ti, err := sess.Get(keyTopics)
	if err != nil {
		topics = make([]string, 0)
	} else {
		topics, ok = ti.([]string)
		if !ok {
			return nil, nil, fmt.Errorf("topics is not a string slice")
		}
	}

	qi, err := sess.Get(keyTopicQos)
	if err != nil {
		qoss = make([]byte, 0)
	} else {
		qoss, ok = qi.([]byte)
		if !ok {
			return nil, nil, fmt.Errorf("QoS is not a byte slice")
		}
	}

	if len(topics) != len(qoss) {
		return nil, nil, fmt.Errorf("Number of topics (%d) != number of QoS (%d)", len(topics), len(qoss))
	}

	return topics, qoss, nil
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func roundUpPowerOfTwo32(n int32) int32 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++

	return n
}

func roundUppowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
