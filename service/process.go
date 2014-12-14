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

	"github.com/dataence/glog"
	"github.com/surge/surgemq/message"
)

func (this *service) processor() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			glog.Errorf("(%d/%s) Recovering from panic: %v", this.id, this.cid, r)
		}

		this.wgStopped.Done()
		this.stop()

		glog.Debugf("(%s) Stopping processor", this.cid)
	}()

	glog.Debugf("(%s) Starting processor", this.cid)

	this.wgStarted.Done()

	for {
		// 1. Find out what message is next and the size of the message
		mtype, total, err := this.peekMessageSize()
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

		msg, _, err := this.peekMessage(mtype, total)
		if err != nil {
			if err != io.EOF {
				glog.Errorf("(%s) Error peeking next message: %v", this.cid, err)
			}
			return
		}

		// 4. Check to see if done is closed, if so, exit
		if this.isDone() {
			return
		}

		//glog.Debugf("(%d/%s) Received: %v", this.id, this.cid, msg)

		// 5. Process the read message
		err = this.processIncoming(msg)
		if err != nil {
			glog.Errorf("(%d/%s) Error processing %s: %v", this.id, this.cid, msg.Name(), err)
		}

		// 6. We should commit the bytes in the buffer so we can move on
		_, err = this.in.ReadCommit(total)
		if err != nil {
			if err != io.EOF {
				glog.Errorf("(%s) Error committing %d read bytes: %v", this.cid, total, err)
			}
			return
		}

		// 7. Check to see if done is closed, if so, exit
		if this.isDone() {
			return
		}
	}
}

func (this *service) processIncoming(msg message.Message) error {
	var err error = nil

	switch msg := msg.(type) {
	case *message.PublishMessage:
		// For PUBLISH message, we should figure out what QoS it is and process accordingly
		// If QoS == 0, we should just take the next step, no ack required
		// If QoS == 1, we should send back PUBACK, then take the next step
		// If QoS == 2, we need to put it in the ack queue, send back PUBREC
		err = this.processPublish(msg)

	case *message.PubackMessage:
		// For PUBACK message, it means QoS 1, we should send to ack queue
		this.pub1ack.ack(msg)
		this.processAcked(this.pub1ack)

	case *message.PubrecMessage:
		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		if err = this.pub2out.ack(msg); err != nil {
			break
		}

		resp := message.NewPubrelMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = this.writeMessage(resp)

	case *message.PubrelMessage:
		// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		if err = this.pub2in.ack(msg); err != nil {
			break
		}

		this.processAcked(this.pub2in)

		resp := message.NewPubcompMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = this.writeMessage(resp)

	case *message.PubcompMessage:
		// For PUBCOMP message, it means QoS 2, we should send to ack queue
		if err = this.pub2out.ack(msg); err != nil {
			break
		}

		this.processAcked(this.pub2out)

	case *message.SubscribeMessage:
		// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
		return this.processSubscribe(msg)

	case *message.SubackMessage:
		// For SUBACK message, we should send to ack queue
		this.suback.ack(msg)
		this.processAcked(this.suback)

	case *message.UnsubscribeMessage:
		// For UNSUBSCRIBE message, we should remove subscriber, then send back UNSUBACK
		return this.processUnsubscribe(msg)

	case *message.UnsubackMessage:
		// For UNSUBACK message, we should send to ack queue
		this.unsuback.ack(msg)
		this.processAcked(this.unsuback)

	case *message.PingreqMessage:
		// For PINGREQ message, we should send back PINGRESP
		resp := message.NewPingrespMessage()
		_, err = this.writeMessage(resp)

	case *message.PingrespMessage:
		this.pingack.ack(msg)
		this.processAcked(this.pingack)

	case *message.DisconnectMessage:
		// For DISCONNECT message, we should quit
		this.stop()

	default:
		return fmt.Errorf("(%s) invalid message type %s.", this.cid, msg.Name())
	}

	if err != nil {
		glog.Debugf("(%d/%s) Error processing acked message: %v", this.id, this.cid, err)
	}

	return err
}

func (this *service) processAcked(ackq *ackqueue) {
	for _, ackmsg := range ackq.acked() {
		// Let's get the messages from the saved message byte slices.
		msg, err := ackmsg.mtype.New()
		if err != nil {
			glog.Errorf("process/processAcked: Unable to creating new %s message: %v", ackmsg.mtype, err)
			continue
		}

		if _, err := msg.Decode(ackmsg.msgbuf); err != nil {
			glog.Errorf("process/processAcked: Unable to decode %s message: %v", ackmsg.mtype, err)
			continue
		}

		ack, err := ackmsg.state.New()
		if err != nil {
			glog.Errorf("process/processAcked: Unable to creating new %s message: %v", ackmsg.state, err)
			continue
		}

		if _, err := ack.Decode(ackmsg.ackbuf); err != nil {
			glog.Errorf("process/processAcked: Unable to decode %s message: %v", ackmsg.state, err)
			continue
		}

		//glog.Debugf("(%d/%s) Processing acked message: %v", this.id, this.cid, ack)

		// - PUBACK if it's QoS 1 message. This is on the client side.
		// - PUBREL if it's QoS 2 message. This is on the server side.
		// - PUBCOMP if it's QoS 2 message. This is on the client side.
		// - SUBACK if it's a subscribe message. This is on the client side.
		// - UNSUBACK if it's a unsubscribe message. This is on the client side.
		switch ackmsg.state {
		case message.PUBREL:
			// If ack is PUBREL, that means the QoS 2 message sent by a remote client is
			// releassed, so let's publish it to other subscribers.
			if err = this.onPublish(msg.(*message.PublishMessage)); err != nil {
				glog.Errorf("(%s) Error processing ack'ed %s message: %v", this.cid, ackmsg.mtype, err)
			}

		case message.PUBACK, message.PUBCOMP, message.SUBACK, message.UNSUBACK, message.PINGRESP:
			// If ack is PUBACK, that means the QoS 1 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PUBCOMP, that means the QoS 2 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is SUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is UNSUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PINGRESP, that means the PINGREQ message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			err = nil

		default:
			glog.Errorf("(%s) Invalid ack message type %s.", this.cid, ackmsg.state)
			continue
		}

		// Call the registered onComplete function
		if ackmsg.onComplete != nil {
			if err := ackmsg.onComplete(msg, ack, err); err != nil {
				glog.Errorf("process/processAcked: Error running onComplete(): %v", err)
			}
		}
	}
}

// For PUBLISH message, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
func (this *service) processPublish(msg *message.PublishMessage) error {
	switch msg.QoS() {
	case message.QosExactlyOnce:
		this.pub2in.wait(msg, nil)

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

// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
func (this *service) processSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubackMessage()
	resp.SetPacketId(msg.PacketId())

	// Subscribe to the different topics
	var retcodes []byte

	topics := msg.Topics()
	qos := msg.Qos()

	for i, t := range topics {
		rqos, err := this.topicsMgr.Subscribe(t, qos[i], &this.onpub)
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

// onServerPublish is the method that gets added to the topic subscribers list by the
// processSubscribe() method. When the server finishes the ack cycle for a PUBLISH
// message, it will call the subscriber, which is this method.
//
// For the server, when this method is called, it means there's a message that should
// be published to the client on the other end of this connection. So we will call
// publish() to send the message.
func (this *service) onServerPublish(msg *message.PublishMessage) error {
	if err := this.publish(msg, nil); err != nil {
		glog.Errorf("process/onServerPublish: Error publishing message: %v", err)
		return err
	}

	return nil
}

func (this *service) onPublish(msg *message.PublishMessage) error {
	err := this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &this.subs, &this.qoss)
	if err != nil {
		glog.Errorf("(%d/%s) Error retrieving subscribers list: %v", this.id, this.cid, err)
		return err
	}

	//glog.Debugf("(%d/%s) Publishing to topic %s and %d subscribers", this.id, this.cid, string(msg.Topic()), len(this.subs))
	for _, s := range this.subs {
		if s != nil {
			fn, ok := s.(*OnPublishFunc)
			if !ok {
				glog.Errorf("Invalid onPublish Function")
				return fmt.Errorf("Invalid onPublish Function")
			} else {
				(*fn)(msg)
			}
		}
	}

	return nil
}
