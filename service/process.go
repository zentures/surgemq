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
	"reflect"

	"github.com/dataence/glog"
	"github.com/surge/surgemq/message"
)

func (this *service) processor() {
	defer func() {
		this.wg.Done()
		this.close()

		glog.Debugf("(%s) Stopping processor", this.cid)
	}()

	glog.Debugf("(%s) Starting processor", this.cid)

	for {
		// 1. Find out what message is next and the size of the message
		mtype, qos, total, err := this.peekMessageSize()
		if err != nil {
			if err != io.EOF {
				glog.Errorf("(%s) Error peeking next message size: %v", this.cid, err)
			}
			return
		}

		// 2. Check to see if done is closed, if so, exit
		if this.isDone() {
			return
		}

		var (
			msg  message.Message
			peek bool = false
		)

		// 3. Depending on the type, we either read the msg into another buffer, or we
		//    peek the message in the existing buffer.
		//
		//    If msg type is PUBLISH QOS 2, then we need to read it because it needs to
		//    be kept in ackqueue for acks, and we want to be able to move forward while
		//    waiting for the acks to come.
		if mtype == message.PUBLISH && qos == message.QosExactlyOnce {
			msg, _, err = this.readMessage(mtype, total)
			if err != nil {
				if err != io.EOF {
					glog.Errorf("(%s) Error reading next message: %v", this.cid, err)
				}
				return
			}
		} else {
			msg, _, err = this.peekMessage(mtype, total)
			if err != nil {
				if err != io.EOF {
					glog.Errorf("(%s) Error peeking next message: %v", this.cid, err)
				}
				return
			}
			peek = true
		}

		// 4. Check to see if done is closed, if so, exit
		if this.isDone() {
			return
		}

		//glog.Debugf("(%d/%s) received %v", this.id, this.cid, msg)

		// 5. Process the read message
		err = this.processIncoming(msg)
		if err != nil {
			glog.Errorf("(%d/%s) Error processing %s: %v", this.id, this.cid, msg.Name(), err)
		}

		// 6. If this was a peek, then we should commit the bytes in the buffer so we
		//    can move on
		if peek {
			_, err := this.in.ReadCommit(total)
			if err != nil {
				if err != io.EOF {
					glog.Errorf("(%s) Error committing %d read bytes: %v", this.cid, total, err)
				}
				return
			}
		}

		// 7. Check to see if done is closed, if so, exit
		if this.isDone() {
			return
		}

		// 8. Finally process all the acked messages. There's acked messages if the
		//    previously processed message is an ack response.
		this.processAcked()
	}
}

func (this *service) isDone() bool {
	select {
	case <-this.done:
		return true

	default:
	}

	return false
}

func (this *service) processAcked() {
	for _, ackmsg := range this.ack.Acked() {
		var err error = nil

		switch ackmsg.msg.Type() {
		case message.PUBLISH:
			// If ack is PUBACK, that means this service sent the message and it got
			// ack'ed. Then don't send it out again. Similarly, if the ack is PUBCOMP,
			// that also means this service sent the message and it should not be
			// send again.
			if ackmsg.ack.Type() != message.PUBCOMP && ackmsg.ack.Type() != message.PUBACK {
				err = this.onPublish(ackmsg.msg.(*message.PublishMessage))
			}

		case message.SUBSCRIBE, message.UNSUBSCRIBE, message.PINGRESP:
			// nothing to do here

		default:
			err = fmt.Errorf("Invalid message type.")
		}

		if err != nil {
			if err != io.EOF {
				glog.Errorf("(%s) Error processing ack'ed %s message: %v", this.cid, ackmsg.msg.Name(), err)
			}
		}

		if ackmsg.onComplete != nil {
			ackmsg.onComplete(ackmsg.msg, ackmsg.ack, err)
		}
	}
}

func (this *service) processIncoming(msg message.Message) error {
	switch msg := msg.(type) {
	case *message.PublishMessage:
		// For PUBLISH message, we should figure out what QoS it is and process accordingly
		// If QoS == 0, we should just take the next step, no ack required
		// If QoS == 1, we should send back PUBACK, then take the next step
		// If QoS == 2, we need to put it in the ack queue, send back PUBREC
		return this.processPublish(msg)

	case *message.PubackMessage:
		// For PUBACK message, it means QoS 1, we should send to ack queue
		this.ack.Ack(msg)

	case *message.PubrecMessage:
		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		return this.processPubrec(msg)

	case *message.PubrelMessage:
		// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		return this.processPubrel(msg)

	case *message.PubcompMessage:
		// For PUBCOMP message, it means QoS 2, we should send to ack queue
		this.ack.Ack(msg)

	case *message.SubscribeMessage:
		// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
		return this.processSubscribe(msg)

	case *message.SubackMessage:
		// For SUBACK message, we should send to ack queue
		this.ack.Ack(msg)

	case *message.UnsubscribeMessage:
		// For UNSUBSCRIBE message, we should remove subscriber, then send back UNSUBACK
		return this.processUnsubscribe(msg)

	case *message.UnsubackMessage:
		// For UNSUBACK message, we should send to ack queue
		this.ack.Ack(msg)

	case *message.PingreqMessage:
		// For PINGREQ message, we should send back PINGRESP
		// Using a closure so we can use defer, too lazy to separate into another function
		return func() error {
			resp := message.NewPingrespMessage()
			_, err := this.writeMessage(resp)
			return err
		}()

	case *message.PingrespMessage:
		this.ack.Ack(msg)

	case *message.DisconnectMessage:
		// For DISCONNECT message, we should quit
		this.Disconnect()

	default:
		return fmt.Errorf("(%s) invalid message type %s.", this.cid, msg.Name())
	}

	return nil
}

// For PUBLISH message, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
func (this *service) processPublish(msg *message.PublishMessage) error {
	switch msg.QoS() {
	case message.QosExactlyOnce:
		this.ack.AckWait(msg, nil)

		resp := message.NewPubrecMessage()
		resp.SetPacketId(msg.PacketId())

		_, err := this.writeMessage(resp)
		return err

	case message.QosAtLeastOnce:
		resp := message.NewPubackMessage()
		resp.SetPacketId(msg.PacketId())

		if _, err := this.writeMessage(resp); err != nil {
			return err
		}

		return this.onPublish(msg)

	case message.QosAtMostOnce:
		return this.onPublish(msg)
	}

	return fmt.Errorf("(%s) invalid message QoS %d.", this.cid, msg.QoS())
}

// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
func (this *service) processPubrec(msg *message.PubrecMessage) error {
	this.ack.Ack(msg)

	resp := message.NewPubrelMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := this.writeMessage(resp)
	return err
}

// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
func (this *service) processPubrel(msg *message.PubrelMessage) error {
	this.ack.Ack(msg)

	resp := message.NewPubcompMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := this.writeMessage(resp)
	return err
}

// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
func (this *service) processSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubackMessage()
	resp.SetPacketId(msg.PacketId())

	// Subscribe to the different topics
	var retcodes []byte

	topics := msg.Topics()
	qos := msg.Qos()

	for i, t := range topics {
		rqos, err := this.topicsMgr.Subscribe(t, qos[i], this)
		if err != nil {
			return err
		}
		addTopic(this.sess, string(t), qos[i])

		retcodes = append(retcodes, rqos)
	}

	if err := resp.AddReturnCodes(retcodes); err != nil {
		return err
	}

	_, err := this.writeMessage(resp)
	return err
}

func (this *service) processUnsubscribe(msg *message.UnsubscribeMessage) error {
	topics := msg.Topics()

	for _, t := range topics {
		this.topicsMgr.Unsubscribe(t, this)
		removeTopic(this.sess, string(t))
	}

	resp := message.NewUnsubackMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := this.writeMessage(resp)
	return err
}

func (this *service) onPublish(msg *message.PublishMessage) error {
	if this.client {
		return this.clientPublish(msg)
	}

	return this.serverPublish(msg)
}

func (this *service) serverPublish(msg *message.PublishMessage) error {
	err := this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &this.subs, &this.qoss)
	if err != nil {
		return err
	}

	for i, s := range this.subs {
		svc, ok := s.(*service)
		if !ok {
			glog.Errorf("Disconnecting client: cannot type assert svc (%s) to *service", reflect.TypeOf(svc).Name())
			this.topicsMgr.Unsubscribe(msg.Topic(), svc)
			svc.Disconnect()
		} else {
			//glog.Debugf("(%d/%s) Publishing to %d/%d %s: %v", this.id, this.cid, i+1, len(this.subs), svc.cid, msg)
			msg.SetPacketId(0)
			msg.SetQoS(this.qoss[i])
			err := svc.Publish(msg, nil)
			if err == ErrBufferNotReady {
				glog.Errorf("Disconnecting client: cannot type assert svc to *service: %s", reflect.TypeOf(svc).Name())
				this.topicsMgr.Unsubscribe(msg.Topic(), svc)
				svc.Disconnect()
			}
		}
	}

	return nil
}

func (this *service) clientPublish(msg *message.PublishMessage) error {
	err := this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &this.subs, &this.qoss)
	if err != nil {
		return err
	}

	for _, s := range this.subs {
		if s != nil {
			fn, ok := s.(OnPublishFunc)
			if !ok {
				glog.Errorf("Invalid onPublish Function")
				return fmt.Errorf("Invalid onPublish Function")
			} else {
				fn(msg)
			}
		}
	}

	return nil
}
