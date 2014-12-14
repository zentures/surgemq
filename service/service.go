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
	"sync"
	"sync/atomic"

	"github.com/dataence/glog"
	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/sessions"
	"github.com/surge/surgemq/topics"
)

const (
	// Queue size for the ack queue
	defaultQueueSize = 16

	keyClientId    = "CLIENTID"
	keyKeepAlive   = "KEEPALIVE"
	keyUsername    = "USERNAME"
	keyWillRetain  = "WILLRETAIN"
	keyWillQos     = "WILLQOS"
	keyWillTopic   = "WILLTOPIC"
	keyWillMessage = "WILLMESSAGE"
	keyVersion     = "VERSION"
	keyTopics      = "TOPICS"
	keyTopicQos    = "TOPICQOS"
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
	this.bytes += n
	this.msgs++
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

	// client ID
	cid string

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

	sessMgr   *sessions.Manager
	topicsMgr *topics.Manager

	// sess is the session object for this MQTT session. It keeps track session variables
	// such as ClientId, KeepAlive, Username, etc
	sess sessions.Session

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

	pub1ack,
	pub2in,
	pub2out,
	suback,
	unsuback,
	pingack *ackqueue

	onpub OnPublishFunc

	// Ack queue. Any messages that require ack'ing are insert here for waiting.
	// - Publish QoS 1 (at least once) messages wait here until puback is received.
	// - Publish QoS 2 (exactly once) messages wait here until the cycle of
	//   publish->pubrec->pubrel->pubcomp cycle is done.
	// - Subscribe messages wait for suback.
	// - Unsubscribe messages wait for unsuback.
	//ack *ackqueue

	inStat  stat
	outStat stat

	intmp  []byte
	outtmp []byte

	subs []interface{}
	qoss []byte
}

func (this *service) start() error {
	var err error

	this.in, err = newBuffer(defaultBufferSize)
	if err != nil {
		return err
	}

	this.out, err = newBuffer(defaultBufferSize)
	if err != nil {
		return err
	}

	this.pub1ack = newAckqueue(defaultQueueSize)
	this.pub2in = newAckqueue(defaultQueueSize)
	this.pub2out = newAckqueue(defaultQueueSize)
	this.suback = newAckqueue(defaultQueueSize)
	this.unsuback = newAckqueue(defaultQueueSize)
	this.pingack = newAckqueue(defaultQueueSize)

	if !this.client {
		this.onpub = func(msg *message.PublishMessage) error {
			if err := this.publish(msg, nil); err != nil {
				glog.Errorf("service/onPublish: Error publishing message: %v", err)
				return err
			}

			return nil
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

	this.wgStarted.Wait()

	return nil
}

// FIXME: The order of closing here causes panic sometimes. For example, if receiver
// calls this, and closes the buffers, somehow it causes buffer.go:476 to panid.
func (this *service) stop() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			glog.Errorf("(%d/%s) Recovering from panic: %v", this.id, this.cid, r)
		}
	}()

	doit := atomic.CompareAndSwapInt64(&this.closed, 0, 1)
	if !doit {
		return
	}

	// Close quit channel, effectively telling all the goroutines it's time to quit
	if this.done != nil {
		glog.Debugf("(%s) closing this.done", this.cid)
		close(this.done)
	}

	// Close the network connection
	if this.conn != nil {
		glog.Debugf("(%s) closing this.conn", this.cid)
		this.conn.Close()
	}

	// Unsubscribe from all the topics for this client, only for the server side though

	if !this.client && this.sess != nil {
		topics, _, err := getTopicsQoss(this.sess)
		if err != nil {
			glog.Errorf("(%s/%d): %v", this.cid, this.id, err)
		} else {
			for _, t := range topics {
				if err := this.topicsMgr.Unsubscribe([]byte(t), &this.onpub); err != nil {
					glog.Errorf("(%d/%s): Error unsubscribing topic %q: %v", this.id, this.cid, t, err)
				}
			}
		}
	}

	topics.Unregister(this.cid)

	// Remove the session from session store
	if this.sessMgr != nil {
		this.sessMgr.Del(this.cid)
	}

	// Close all the buffers and queues
	if this.in != nil {
		glog.Debugf("(%s) closing this.in", this.cid)
		this.in.Close()
	}

	if this.out != nil {
		glog.Debugf("(%s) closing this.out", this.cid)
		this.out.Close()
	}

	this.wgStopped.Wait()

	this.conn = nil
	this.in = nil
	this.out = nil
}

func (this *service) publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid, msg.Name(), err)
	}

	switch msg.QoS() {
	case message.QosAtMostOnce:
		if onComplete != nil {
			return onComplete(msg, nil, nil)
		}

		return nil

	case message.QosAtLeastOnce:
		return this.pub1ack.wait(msg, onComplete)

	case message.QosExactlyOnce:
		return this.pub2out.wait(msg, onComplete)
	}

	return nil
}

func (this *service) subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	if onPublish == nil {
		return fmt.Errorf("onPublish function is nil. No need to subscribe.")
	}

	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid, msg.Name(), err)
	}

	onc := func(msg, ack message.Message, err error) error {
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
				addTopic(this.sess, string(t), c)
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

	return this.suback.wait(msg, onc)
}

func (this *service) unsubscribe(msg *message.UnsubscribeMessage, onComplete OnCompleteFunc) error {
	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid, msg.Name(), err)
	}

	onc := func(msg, ack message.Message, err error) error {
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

			removeTopic(this.sess, string(tb))
		}

		if onComplete != nil {
			return onComplete(msg, ack, err2)
		}

		return err2
	}

	return this.unsuback.wait(msg, onc)
}

func (this *service) ping(onComplete OnCompleteFunc) error {
	msg := message.NewPingreqMessage()

	_, err := this.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s message: %v", this.cid, msg.Name(), err)
	}

	return this.pingack.wait(msg, onComplete)
}

func (this *service) isDone() bool {
	select {
	case <-this.done:
		return true

	default:
	}

	return false
}
