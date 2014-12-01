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
	"time"

	"github.com/dataence/glog"
	"github.com/gorilla/websocket"
	"github.com/surge/surgemq/message"
)

func (this *service) receiver() {
	var (
		err error
	)

	defer func() {
		//if err != nil {
		//	glog.Errorf("(%d) %v", this.id, err)
		//}
		this.wg.Done()
		this.close()

		glog.Debugf("(%d) Stopping receiver", this.id)
	}()

	glog.Debugf("(%d) Starting receiver", this.id)

	switch conn := this.conn.(type) {
	case net.Conn:
		conn.SetReadDeadline(time.Now().Add(this.ctx.KeepAlive))

		for {
			//n, err = io.Copy(this.in, conn)
			_, err = this.in.ReadFrom(conn)

			//glog.Debugf("(%d) read %d bytes", this.id, n)

			//if isTimeout(err) || err == io.EOF {
			if err != nil {
				return
			}
		}

	case *websocket.Conn:
		glog.Errorf("(%d) Websocket: %v", this.id, ErrInvalidConnectionType)

	default:
		glog.Errorf("(%d) %v", this.id, ErrInvalidConnectionType)
	}
}

func (this *service) sender() {
	defer func() {
		this.wg.Done()
		this.close()

		glog.Debugf("(%d) Stopping sender", this.id)
	}()

	glog.Debugf("(%d) Starting sender", this.id)

	switch conn := this.conn.(type) {
	case net.Conn:
		for {
			_, err := this.out.WriteTo(conn)

			//glog.Debugf("(%d) wrote %d bytes", this.id, n)

			if err != nil {
				if err != io.EOF {
					glog.Errorf("(%d) error writing data: %v", this.id, err)
				}
				return
			}
		}

	case *websocket.Conn:
		glog.Errorf("(%s) Websocket not supported", this.cid)

	default:
		glog.Errorf("(%s) Invalid connection type", this.cid)
	}
}

func (this *service) peekMessageSize() (message.MessageType, byte, int, error) {
	var (
		b   []byte
		err error
		cnt int = 2
	)

	//defer func() {
	//	if err != nil {
	//		glog.Errorf("(%d) %v", this.id, err)
	//	}
	//}()

	if this.in == nil {
		err = ErrBufferNotReady
		return 0, 0, 0, err
	}

	// Let's read enough bytes to get the message header (msg type, remaining length)
	for {
		// If we have read 5 bytes and still not done, then there's a problem.
		if cnt > 5 {
			return 0, 0, 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
		}

		// Peek cnt bytes from the input buffer.
		b, err = this.in.ReadWait(cnt)
		if err != nil {
			return 0, 0, 0, err
		}

		// If not enough bytes are returned, then continue until there's enough.
		if len(b) < cnt {
			continue
		}

		// If we got enough bytes, then check the last byte to see if the continuation
		// bit is set. If so, increment cnt and continue peeking
		if b[cnt-1] >= 0x80 {
			cnt++
		} else {
			break
		}
	}

	// Get the remaining length of the message
	remlen, m := binary.Uvarint(b[1:])

	// Total message length is remlen + 1 (msg type) + m (remlen bytes)
	total := int(remlen) + 1 + m

	mtype := message.MessageType(b[0] >> 4)
	//glog.Debugf("(%d) peeked %s, total = %d, remlen = %d", this.id, mtype.Name(), total, remlen)

	qos := ((b[0] & 0x0f) >> 1) & 0x3

	return mtype, qos, total, err
}

func (this *service) peekMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b    []byte
		err  error
		i, n int
		msg  message.Message
	)

	//defer func() {
	//	if err != nil {
	//		glog.Errorf("(%d) %v", this.id, err)
	//	}
	//}()

	if this.in == nil {
		err = ErrBufferNotReady
		return nil, 0, err
	}

	// Peek until we get total bytes
	for i = 0; ; i++ {
		// Peek remlen bytes from the input buffer.
		b, err = this.in.ReadWait(total)
		if err != nil && err != ErrBufferInsufficientData {
			return nil, 0, err
		}

		// If not enough bytes are returned, then continue until there's enough.
		if len(b) >= total {
			break
		}
	}

	//glog.Debugf("(%d) Peeked %d times for %s message of %d bytes", this.id, i, mtype.Name(), len(b))

	//glog.Debugf("(%d) got %s, total = %d", this.id, mtype.Name(), len(b))

	msg, err = mtype.New()
	if err != nil {
		return nil, 0, err
	}

	n, err = msg.Decode(b)
	return msg, n, err
}

func (this *service) readMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b   []byte
		err error
		n   int
		msg message.Message
	)

	//defer func() {
	//	if err != nil {
	//		glog.Errorf("(%d) %v", this.id, err)
	//	}
	//}()

	if this.in == nil {
		err = ErrBufferNotReady
		return nil, 0, err
	}

	if len(this.intmp) < total {
		this.intmp = make([]byte, total)
	}

	// Read until we get total bytes
	l := 0
	for l < total {
		n, err = this.in.Read(this.intmp[l:])
		l += n
		if err != nil {
			return nil, 0, err
		}
	}

	b = this.intmp[:total]

	msg, err = mtype.New()
	if err != nil {
		return msg, 0, err
	}

	n, err = msg.Decode(b)
	return msg, n, err
}

func (this *service) writeMessage(msg message.Message) (int, error) {
	var (
		l    int = msg.Len()
		m, n int
		err  error
		buf  []byte
		wrap bool
	)

	//defer func() {
	//	if err != nil {
	//		glog.Errorf("(%d) %v", this.id, err)
	//	}
	//}()

	if this.out == nil {
		err = ErrBufferNotReady
		return 0, err
	}

	buf, wrap, err = this.out.WriteWait(l)
	if err != nil {
		return 0, err
	}

	glog.Debugf("(%d) Sending %s message of %d bytes", this.id, msg.Name(), l)
	if wrap {
		if len(this.outtmp) < l {
			this.outtmp = make([]byte, l)
		}

		n, err = msg.Encode(this.outtmp)
		if err != nil {
			//glog.Debugf("(%d) error encoding message: %v", this.id, err)
			return 0, err
		}

		m, err = this.out.Write(this.outtmp[0:n])
		if err != nil {
			//glog.Debugf("(%d) error writing message: %v", this.id, err)
			return m, err
		}
	} else {
		n, err = msg.Encode(buf)
		if err != nil {
			return 0, err
		}

		m, err = this.out.WriteCommit(n)
		if err != nil {
			return 0, err
		}
	}

	return m, nil
}

func (this *service) commitRead(buf *buffer, n int) (int, error) {
	if buf == nil {
		return 0, ErrBufferNotReady
	}

	//glog.Debugf("(%d) Commiting %d bytes", this.id, n)
	return buf.ReadCommit(n)
}
