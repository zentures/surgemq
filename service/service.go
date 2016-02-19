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
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/surge/glog"
	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/sessions"
	"github.com/surgemq/surgemq/topics"
)

type (
	OnCompleteFunc func(msg, ack message.Message, err error) error
	OnPublishFunc  func(msg *message.PublishMessage) error
)

type stat struct {
	bytes int64
	msgs  int64
}

func (this *stat) increment(n int64) {
	atomic.AddInt64(&this.bytes, n)
	atomic.AddInt64(&this.msgs, 1)
}

var (
	gsvcid uint64 = 0
)

type service struct {
	// The ID of this service, it's not related to the Client ID, just a number that's
	// incremented for every new service.
	id uint64

	// Is this a client or server. It's set by either Connect (client) or
	// HandleConnection (server).
	client bool

	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
	keepAlive int

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	connectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	ackTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	timeoutRetries int

	// Network connection for this service
	conn io.Closer

	// Session manager for tracking all the clients
	sessMgr *sessions.Manager

	// Topics manager for all the client subscriptions
	topicsMgr *topics.Manager

	// sess is the session object for this MQTT session. It keeps track session variables
	// such as ClientId, KeepAlive, Username, etc
	sess *sessions.Session

	// Wait for the various goroutines to finish starting and stopping
	wgStarted sync.WaitGroup
	wgStopped sync.WaitGroup

	// writeMessage mutex - serializes writes to the outgoing buffer.
	wmu sync.Mutex

	// Whether this is service is closed or not.
	closed int64

	// Quit signal for determining when this service should end. If channel is closed,
	// then exit.
	done chan struct{}

	// Incoming data buffer. Bytes are read from the connection and put in here.
	in *buffer

	// Outgoing data buffer. Bytes written here are in turn written out to the connection.
	out *buffer

	// onpub is the method that gets added to the topic subscribers list by the
	// processSubscribe() method. When the server finishes the ack cycle for a
	// PUBLISH message, it will call the subscriber, which is this method.
	//
	// For the server, when this method is called, it means there's a message that
	// should be published to the client on the other end of this connection. So we
	// will call publish() to send the message.
	onpub OnPublishFunc

	inStat  stat
	outStat stat

	intmp  []byte
	outtmp []byte

	subs  []interface{}
	qoss  []byte
	rmsgs []*message.PublishMessage
}

func (this *service) start() error {
	var err error

	// Create the incoming ring buffer
	this.in, err = newBuffer(defaultBufferSize)
	if err != nil {
		return err
	}

	// Create the outgoing ring buffer
	this.out, err = newBuffer(defaultBufferSize)
	if err != nil {
		return err
	}

	// If this is a server
	if !this.client {
		// Creat the onPublishFunc so it can be used for published messages
		this.onpub = func(msg *message.PublishMessage) error {
			if err := this.publish(msg, nil); err != nil {
				glog.Errorf("service/onPublish: Error publishing message: %v", err)
				return err
			}

			return nil
		}

		// If this is a recovered session, then add any topics it subscribed before
		topics, qoss, err := this.sess.Topics()
		if err != nil {
			return err
		} else {
			for i, t := range topics {
				this.topicsMgr.Subscribe([]byte(t), qoss[i], &this.onpub)
			}
		}
	}

	// Processor is responsible for reading messages out of the buffer and processing
	// them accordingly.
	this.wgStarted.Add(1)
	this.wgStopped.Add(1)
	go this.processor()

	// Receiver is responsible for reading from the connection and putting data into
	// a buffer.
	this.wgStarted.Add(1)
	this.wgStopped.Add(1)
	go this.receiver()

	// Sender is responsible for writing data in the buffer into the connection.
	this.wgStarted.Add(1)
	this.wgStopped.Add(1)
	go this.sender()

	// Wait for all the goroutines to start before returning
	this.wgStarted.Wait()

	return nil
}

// FIXME: The order of closing here causes panic sometimes. For example, if receiver
// calls this, and closes the buffers, somehow it causes buffer.go:476 to panid.
func (this *service) stop() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			glog.Errorf("(%s) Recovering from panic: %v", this.cid(), r)
		}
	}()

	doit := atomic.CompareAndSwapInt64(&this.closed, 0, 1)
	if !doit {
		return
	}

	// Close quit channel, effectively telling all the goroutines it's time to quit
	if this.done != nil {
		glog.Debugf("(%s) closing this.done", this.cid())
		close(this.done)
	}

	// Close the network connection
	if this.conn != nil {
		glog.Debugf("(%s) closing this.conn", this.cid())
		this.conn.Close()
	}

	this.in.Close()
	this.out.Close()

	// Wait for all the goroutines to stop.
	this.wgStopped.Wait()

	glog.Debugf("(%s) Received %d bytes in %d messages.", this.cid(), this.inStat.bytes, this.inStat.msgs)
	glog.Debugf("(%s) Sent %d bytes in %d messages.", this.cid(), this.outStat.bytes, this.outStat.msgs)

	// Unsubscribe from all the topics for this client, only for the server side though
	if !this.client && this.sess != nil {
		topics, _, err := this.sess.Topics()
		if err != nil {
			glog.Errorf("(%s/%d): %v", this.cid(), this.id, err)
		} else {
			for _, t := range topics {
				if err := this.topicsMgr.Unsubscribe([]byte(t), &this.onpub); err != nil {
					glog.Errorf("(%s): Error unsubscribing topic %q: %v", this.cid(), t, err)
				}
			}
		}
	}

	// Publish will message if WillFlag is set. Server side only.
	if !this.client && this.sess.Cmsg.WillFlag() {
		glog.Infof("(%s) service/stop: connection unexpectedly closed. Sending Will.", this.cid())
		this.onPublish(this.sess.Will)
	}

	// Remove the client topics manager
	if this.client {
		topics.Unregister(this.sess.ID())
	}

	// Remove the session from session store if it's suppose to be clean session
	if this.sess.Cmsg.CleanSession() && this.sessMgr != nil {
		this.sessMgr.Del(this.sess.ID())
	}

	this.conn = nil
	this.in = nil
	this.out = nil
}

func (this *service) publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	//glog.Debugf("service/publish: Publishing %s", msg)
	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid(), msg.Name(), err)
	}

	switch msg.QoS() {
	case message.QosAtMostOnce:
		if onComplete != nil {
			return onComplete(msg, nil, nil)
		}

		return nil

	case message.QosAtLeastOnce:
		return this.sess.Pub1ack.Wait(msg, onComplete)

	case message.QosExactlyOnce:
		return this.sess.Pub2out.Wait(msg, onComplete)
	}

	return nil
}

func (this *service) subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	if onPublish == nil {
		return fmt.Errorf("onPublish function is nil. No need to subscribe.")
	}

	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid(), msg.Name(), err)
	}

	var onc OnCompleteFunc = func(msg, ack message.Message, err error) error {
		onComplete := onComplete
		onPublish := onPublish

		if err != nil {
			if onComplete != nil {
				return onComplete(msg, ack, err)
			}
			return err
		}

		sub, ok := msg.(*message.SubscribeMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid SubscribeMessage received"))
			}
			return nil
		}

		suback, ok := ack.(*message.SubackMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid SubackMessage received"))
			}
			return nil
		}

		if sub.PacketId() != suback.PacketId() {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Sub and Suback packet ID not the same. %d != %d.", sub.PacketId(), suback.PacketId()))
			}
			return nil
		}

		retcodes := suback.ReturnCodes()
		topics := sub.Topics()

		if len(topics) != len(retcodes) {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Incorrect number of return codes received. Expecting %d, got %d.", len(topics), len(retcodes)))
			}
			return nil
		}

		var err2 error = nil

		for i, t := range topics {
			c := retcodes[i]

			if c == message.QosFailure {
				err2 = fmt.Errorf("Failed to subscribe to '%s'\n%v", string(t), err2)
			} else {
				this.sess.AddTopic(string(t), c)
				_, err := this.topicsMgr.Subscribe(t, c, &onPublish)
				if err != nil {
					err2 = fmt.Errorf("Failed to subscribe to '%s' (%v)\n%v", string(t), err, err2)
				}
			}
		}

		if onComplete != nil {
			return onComplete(msg, ack, err2)
		}

		return err2
	}

	return this.sess.Suback.Wait(msg, onc)
}

func (this *service) unsubscribe(msg *message.UnsubscribeMessage, onComplete OnCompleteFunc) error {
	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid(), msg.Name(), err)
	}

	var onc OnCompleteFunc = func(msg, ack message.Message, err error) error {
		onComplete := onComplete

		if err != nil {
			if onComplete != nil {
				return onComplete(msg, ack, err)
			}
			return err
		}

		unsub, ok := msg.(*message.UnsubscribeMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid UnsubscribeMessage received"))
			}
			return nil
		}

		unsuback, ok := ack.(*message.UnsubackMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid UnsubackMessage received"))
			}
			return nil
		}

		if unsub.PacketId() != unsuback.PacketId() {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Unsub and Unsuback packet ID not the same. %d != %d.", unsub.PacketId(), unsuback.PacketId()))
			}
			return nil
		}

		var err2 error = nil

		for _, tb := range unsub.Topics() {
			// Remove all subscribers, which basically it's just this client, since
			// each client has it's own topic tree.
			err := this.topicsMgr.Unsubscribe(tb, nil)
			if err != nil {
				err2 = fmt.Errorf("%v\n%v", err2, err)
			}

			this.sess.RemoveTopic(string(tb))
		}

		if onComplete != nil {
			return onComplete(msg, ack, err2)
		}

		return err2
	}

	return this.sess.Unsuback.Wait(msg, onc)
}

func (this *service) ping(onComplete OnCompleteFunc) error {
	msg := message.NewPingreqMessage()

	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid(), msg.Name(), err)
	}

	return this.sess.Pingack.Wait(msg, onComplete)
}

func (this *service) isDone() bool {
	select {
	case <-this.done:
		return true

	default:
	}

	return false
}

func (this *service) cid() string {
	return fmt.Sprintf("%d/%s", this.id, this.sess.ID())
}
