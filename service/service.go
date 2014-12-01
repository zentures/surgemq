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
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dataence/glog"
	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/sessions"
)

type (
	OnCompleteFunc func(msg, ack message.Message, err error)
	OnPublishFunc  func(msg *message.PublishMessage) error
)

type stat struct {
	bytes int64
	msgs  int64
}

func (this *stat) increment(n int64) {
	this.bytes += n
	this.msgs++
}

const (
	// Queue size for the input queue
	defaultQueueSize = 16

	defaultBufferSize = 1024 * 256
)

var (
	gsvcid uint64 = 0
)

type service struct {
	id uint64

	ctx Context

	// Is this a client or server. It's set by either Connect (client) or
	// HandleConnection (server).
	client bool

	// client ID
	cid string

	conn io.Closer

	// sess is the session object for this MQTT session. It keeps track session variables
	// such as ClientId, KeepAlive, Username, etc
	sess *sessions.Session

	// Wait for the various goroutines to finish
	wg sync.WaitGroup

	// Whether this is service is closed or not.
	closed int64

	// Quit signal for determining when this service should end. If channel is closed,
	// then exit.
	done chan struct{}

	// Incoming data buffer. Bytes are read from the connection and put in here.
	in *buffer

	// Outgoing data buffer. Bytes written here are in turn written out to the connection.
	out *buffer

	// Ack queue. Any messages that require ack'ing are insert here for waiting.
	// - Publish QoS 1 (at least once) messages wait here until puback is received.
	// - Publish QoS 2 (exactly once) messages wait here until the cycle of
	//   publish->pubrec->pubrel->pubcomp cycle is done.
	// - Subscribe messages wait for suback.
	// - Unsubscribe messages wait for unsuback.
	ack *ackqueue

	inStat  stat
	outStat stat

	intmp  []byte
	outtmp []byte
}

func (this *service) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	glog.Debugf("(%d) sending %s qos = %d pktid = %d", this.id, msg.Name(), msg.QoS(), msg.PacketId())
	if msg.QoS() == 0 {
		_, err := this.writeMessage(msg)
		if err != nil {
			//glog.Errorf("(%d) Error sending publish message: %v", this.id, err)
			return err
		}

		if onComplete != nil {
			onComplete(msg, nil, nil)
		}

		return nil
	}

	return this.sendAndAckWait(msg, onComplete)
}

func (this *service) Subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	if onPublish == nil {
		return fmt.Errorf("onPublish function is nil. No need to subscribe.")
	}

	return this.sendAndAckWait(msg, func(msg, ack message.Message, err error) {
		if err != nil {
			if onComplete != nil {
				onComplete(msg, ack, err)
			}
		}

		sub, ok := msg.(*message.SubscribeMessage)
		if !ok {
			if onComplete != nil {
				onComplete(msg, ack, fmt.Errorf("Invalid SubscribeMessage received"))
			}
			return
		}

		suback, ok := ack.(*message.SubackMessage)
		if !ok {
			if onComplete != nil {
				onComplete(msg, ack, fmt.Errorf("Invalid SubackMessage received"))
			}
			return
		}

		if sub.PacketId() != suback.PacketId() {
			if onComplete != nil {
				onComplete(msg, ack, fmt.Errorf("Sub and Suback packet ID not the same. %d != %d.", sub.PacketId(), suback.PacketId()))
			}
			return
		}

		retcodes := suback.ReturnCodes()
		topics := sub.Topics()

		if len(topics) != len(retcodes) {
			if onComplete != nil {
				onComplete(msg, ack, fmt.Errorf("Incorrect number of return codes received. Expecting %d, got %d.", len(topics), len(retcodes)))
			}
			return
		}

		var err2 error = nil

		for i, t := range topics {
			c := retcodes[i]

			if c == message.QosFailure {
				err2 = fmt.Errorf("Failed to subscribe to '%s'\n%v", string(t), err2)
			} else {
				this.sess.AddTopic(string(t), c)
				_, err := this.ctx.Topics.Subscribe(t, c, onPublish)
				if err != nil {
					err2 = fmt.Errorf("Failed to subscribe to '%s' (%v)\n%v", string(t), err, err2)
				}
			}
		}

		if onComplete != nil {
			onComplete(msg, ack, err2)
		}
	})
}

func (this *service) Unsubscribe(msg *message.UnsubscribeMessage, onComplete OnCompleteFunc) error {
	return this.sendAndAckWait(msg, func(msg, ack message.Message, err error) {
		if err != nil {
			if onComplete != nil {
				onComplete(msg, ack, err)
			}
		}

		unsub, ok := msg.(*message.UnsubscribeMessage)
		if !ok {
			if onComplete != nil {
				onComplete(msg, ack, fmt.Errorf("Invalid UnsubscribeMessage received"))
			}
			return
		}

		unsuback, ok := ack.(*message.UnsubackMessage)
		if !ok {
			if onComplete != nil {
				onComplete(msg, ack, fmt.Errorf("Invalid UnsubackMessage received"))
			}
			return
		}

		if unsub.PacketId() != unsuback.PacketId() {
			if onComplete != nil {
				onComplete(msg, ack, fmt.Errorf("Unsub and Unsuback packet ID not the same. %d != %d.", unsub.PacketId(), unsuback.PacketId()))
			}
			return
		}

		var err2 error = nil

		for _, tb := range unsub.Topics() {
			// Remove all subscribers, which basically it's just this client, since
			// each client has it's own topic tree.
			err := this.ctx.Topics.Unsubscribe(tb, nil)
			if err != nil {
				err2 = fmt.Errorf("%v\n%v", err2, err)
			}

			this.sess.RemoveTopic(string(tb))
		}

		if onComplete != nil {
			onComplete(msg, ack, err2)
		} else {
			glog.Errorf("%v", err2)
		}
	})
}

func (this *service) Ping(onComplete OnCompleteFunc) error {
	msg := message.NewPingreqMessage()
	return this.sendAndAckWait(msg, onComplete)
}

func (this *service) Disconnect() {
	this.close()
}

func (this *service) close() {
	doit := atomic.CompareAndSwapInt64(&this.closed, 0, 1)
	if !doit {
		return
	}

	// Close quit channel, effectively telling all the goroutines it's time to quit
	if this.done != nil {
		glog.Debugf("(%d) closing this.done", this.id)
		close(this.done)
	}

	// Close the network connection
	if this.conn != nil {
		glog.Debugf("(%d) closing this.conn", this.id)
		this.conn.Close()
	}

	// Unsubscribe from all the topics for this client
	if this.sess != nil {
		for _, t := range this.sess.Topics {
			this.ctx.Topics.Unsubscribe([]byte(t), this)
		}
	}

	// Remove the session from session store
	this.ctx.Store.Del(this.cid)

	// Close all the buffers and queues
	if this.in != nil {
		glog.Debugf("(%d) closing this.in", this.id)
		this.in.Close()
	}

	if this.out != nil {
		glog.Debugf("(%d) closing this.out", this.id)
		this.out.Close()
	}

	this.wg.Wait()

	this.conn = nil
	this.in = nil
	this.out = nil
}

// Copied from http://golang.org/src/pkg/net/timeout_test.go
func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

func (this *service) initSendRecv() error {
	var err error

	this.in, err = newBuffer(defaultBufferSize)
	if err != nil {
		return err
	}

	if this.client {
		glog.Debugf("(%d) client input buffer %d", this.id, this.in.ID())
	} else {
		glog.Debugf("(%d) server input buffer %d", this.id, this.in.ID())
	}

	this.out, err = newBuffer(defaultBufferSize)
	if err != nil {
		return err
	}

	if this.client {
		glog.Debugf("(%d) client output buffer %d", this.id, this.out.ID())
	} else {
		glog.Debugf("(%d) server output buffer %d", this.id, this.out.ID())
	}

	// Receiver is responsible for reading from the connection and putting data into
	// a buffer.
	this.wg.Add(1)
	go this.receiver()

	// Sender is responsible for writing data in the buffer into the connection.
	this.wg.Add(1)
	go this.sender()

	return nil
}

func (this *service) initProcessor() error {
	this.ack = newAckqueue(defaultQueueSize)

	// Processor is responsible for reading messages out of the buffer and processing
	// them accordingly.
	this.wg.Add(1)
	go this.processor()

	return nil
}

func (this *service) connectClient() error {

	// To establish a connection, we must
	// 1. Read and decode the message.ConnectMessage from the wire
	// 2. If no decoding errors, then authenticate using username and password.
	//    Otherwise, write out to the wire message.ConnackMessage with
	//    appropriate error.
	// 3. If authentication is successful, then either create a new session or
	//    retrieve existing session
	// 4. Write out to the wire a successful message.ConnackMessage message

	// Read the CONNECT message from the wire, if error, then check to see if it's
	// a CONNACK error. If it's CONNACK error, send the proper CONNACK error back
	// to client. Exit regardless of error type.

	resp := message.NewConnackMessage()

	mtype, _, total, err := this.peekMessageSize()
	if err != nil {
		return err
	}

	if mtype != message.CONNECT {
		return fmt.Errorf("Received invalid CONNACK message")
	}

	mreq, nreq, err := this.peekMessage(mtype, total)
	if err != nil {
		if cerr, ok := err.(message.ConnackCode); ok {
			resp.SetReturnCode(cerr)
			resp.SetSessionPresent(false)
			this.writeMessage(resp)
		}
		return err
	}
	defer this.commitRead(this.in, nreq)

	req, ok := mreq.(*message.ConnectMessage)
	if !ok {
		return fmt.Errorf("Received invalid CONNECT message")
	}

	// Authenticate the user, if error, return error and exit
	err = this.ctx.Auth.Authenticate(string(req.Username()), string(req.Password()))
	if err != nil {
		resp.SetReturnCode(message.ErrBadUsernameOrPassword)
		resp.SetSessionPresent(false)
		_, err := this.writeMessage(resp)
		return err
	}

	err = this.getServerSession(req, resp)
	if err != nil {
		return err
	}

	resp.SetReturnCode(message.ConnectionAccepted)

	nresp, err := this.writeMessage(resp)
	if err != nil {
		return err
	}

	this.inStat.increment(int64(nreq))
	this.outStat.increment(int64(nresp))

	return nil
}

func (this *service) getServerSession(req *message.ConnectMessage, resp *message.ConnackMessage) error {
	// If CleanSession is set to 0, the server MUST resume communications with the
	// client based on state from the current session, as identified by the client
	// identifier. If there is no session associated with the client identifier the
	// server must create a new session.
	//
	// If CleanSession is set to 1, the client and server must discard any previous
	// session and start a new one. This session lasts as long as the network c
	// onnection. State data associated with this session must not be reused in any
	// subsequent session.

	var err error

	// Check to see if the client supplied an ID, if not, generate one and set
	// clean session.
	if len(req.ClientId()) == 0 {
		this.cid = fmt.Sprintf("internalclient--%d--", this.id)
		req.SetClientId([]byte(this.cid))
		req.SetCleanSession(true)
	} else {
		this.cid = string(req.ClientId())
	}

	// If CleanSession is NOT set, check the session store for existing session.
	// If found, return it.
	if !req.CleanSession() {
		if this.sess, err = this.ctx.Store.Get(this.cid); err == nil {
			resp.SetSessionPresent(true)
		}
	}

	// If CleanSession, or no existing session found, then create a new one
	if this.sess == nil {
		if this.sess, err = this.ctx.Store.New(this.cid); err != nil {
			return err
		}

		resp.SetSessionPresent(false)
	}

	this.sess.ClientId = string(this.cid)
	this.sess.KeepAlive = time.Duration(req.KeepAlive())
	this.sess.Username = string(req.Username())
	this.sess.WillRetain = req.WillRetain()
	this.sess.WillQos = req.WillQos()
	this.sess.WillTopic = string(req.WillTopic())
	this.sess.WillMessage = string(req.WillMessage())
	this.sess.Version = req.Version()

	for i, t := range this.sess.Topics {
		this.ctx.Topics.Subscribe([]byte(t), this.sess.Qos[i], this)
	}

	//glog.Debugf("(%d) client connected", this.id)

	return nil
}

func (this *service) connectToServer(msg *message.ConnectMessage) error {
	conn, ok := this.conn.(net.Conn)
	if !ok {
		return ErrInvalidConnectionType
	}

	nreq, err := this.writeMessage(msg)
	if err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(this.ctx.ConnectTimeout))

	mtype, _, total, err := this.peekMessageSize()
	if err != nil {
		return err
	}

	if mtype != message.CONNACK {
		return fmt.Errorf("Received invalid CONNACK message")
	}

	mresp, nresp, err := this.peekMessage(mtype, total)
	if err != nil {
		return err
	}
	defer this.commitRead(this.in, nresp)

	resp, ok := mresp.(*message.ConnackMessage)
	if !ok {
		return fmt.Errorf("Received invalid CONNACK message")
	}

	ret := resp.ReturnCode()
	if ret != message.ConnectionAccepted {
		return ret
	}

	err = this.getClientSession(msg, resp)
	if err != nil {
		return err
	}

	//glog.Debugf("(%d) client return code = %s", this.id, resp.ReturnCode().Desc())

	this.inStat.increment(int64(nreq))
	this.outStat.increment(int64(nresp))

	conn.SetReadDeadline(time.Now().Add(this.ctx.KeepAlive))

	return nil
}

func (this *service) getClientSession(req *message.ConnectMessage, resp *message.ConnackMessage) error {
	var err error

	if this.sess == nil {
		if this.sess, err = this.ctx.Store.New(this.cid); err != nil {
			return err
		}
	}

	this.sess.ClientId = string(this.cid)
	this.sess.KeepAlive = time.Duration(req.KeepAlive())
	this.sess.Username = string(req.Username())
	this.sess.WillRetain = req.WillRetain()
	this.sess.WillQos = req.WillQos()
	this.sess.WillTopic = string(req.WillTopic())
	this.sess.WillMessage = string(req.WillMessage())
	this.sess.Version = req.Version()

	return nil
}

func (this *service) sendAndAckWait(msg message.Message, onComplete OnCompleteFunc) error {
	// For any message that needs to go wait in the ackqueue, we should make a copy of that
	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid, msg.Name(), err)
	}

	return this.ack.AckWait(msg, onComplete)
}
