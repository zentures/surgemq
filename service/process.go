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
		this.wg.Done()
		this.close()

		glog.Debugf("(%d) Stopping processor", this.id)
	}()

	glog.Debugf("(%d) Starting processor", this.id)

	for {
		//glog.Debugf("(%d) Waiting for next message", this.id)

		// 1. Find out what message is next and the size of the message
		mtype, qos, total, err := this.peekMessageSize()
		if err != nil {
			if err != io.EOF {
				glog.Errorf("(%d) Error peeking next message size: %v", this.id, err)
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
					glog.Errorf("(%d) Error reading next message: %v", this.id, err)
				}
				return
			}
		} else {
			msg, _, err = this.peekMessage(mtype, total)
			if err != nil {
				if err != io.EOF {
					glog.Errorf("(%d) Error peeking next message: %v", this.id, err)
				}
				return
			}
			peek = true
		}

		// 4. Check to see if done is closed, if so, exit
		if this.isDone() {
			return
		}

		if this.client {
			glog.Debugf("(%d) got %s msg len = %d", this.id, msg.Name(), msg.Len())
		}

		// 5. Process the read message
		err = this.processIncoming(msg)
		if err != nil {
			glog.Errorf("(%d) Error processing %s: %v", msg.Name(), err)
		}

		// 6. If this was a peek, then we should commit the bytes in the buffer so we
		//    can move on
		if peek {
			_, err := this.commitRead(this.in, total)
			if err != nil {
				if err != io.EOF {
					glog.Errorf("(%d) Error committing %d read bytes: %v", this.id, total, err)
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

		glog.Debugf("(%d) %s got acked", this.id, ackmsg.msg.Name())

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
	//if !this.client {
	//glog.Debugf("(%d) Processing %s message", this.id, msg.Name())
	//}

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
	//glog.Debugf("(%d) Received %s, topic = %s, qos = %d, pktid = %d", this.id, msg.Name(), string(msg.Topic()), msg.QoS(), msg.PacketId())

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
	//glog.Debugf("(%s) Received %s %d", this.cid, msg.Name(), msg.PacketId())

	this.ack.Ack(msg)

	resp := message.NewPubrelMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := this.writeMessage(resp)
	return err
}

// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
func (this *service) processPubrel(msg *message.PubrelMessage) error {
	//glog.Debugf("(%d) Received %s %d", this.id, msg.Name(), msg.PacketId())

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
		//glog.Debugf("(%d) Subscribing to %s (%d)", this.id, string(t), qos[i])
		rqos, err := this.ctx.Topics.Subscribe(t, qos[i], this)
		if err != nil {
			return err
		}
		this.sess.AddTopic(string(t), qos[i])

		//if rqos == message.QosFailure {
		//	glog.Errorf("(%d) error subscribing to topic %s, qos %d", this.id, t, qos)
		//}

		retcodes = append(retcodes, rqos)
	}

	if err := resp.AddReturnCodes(retcodes); err != nil {
		//glog.Errorf("(%d) error adding return code: %v", this.id, err)
		return err
	}

	_, err := this.writeMessage(resp)
	return err
}

func (this *service) processUnsubscribe(msg *message.UnsubscribeMessage) error {
	topics := msg.Topics()

	for _, t := range topics {
		//glog.Debugf("(%d) Unsubscribing to %s", this.id, string(t))
		err := this.ctx.Topics.Unsubscribe(t, this)
		if err != nil {
			//glog.Debugf("(%d) err = %v", this.id, err)
		}
		this.sess.RemoveTopic(string(t))
	}

	resp := message.NewUnsubackMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := this.writeMessage(resp)
	return err
}

func (this *service) onPublish(msg *message.PublishMessage) error {
	//glog.Debugf("(%d) onPublish %d", msg.PacketId())

	if this.client {
		return this.clientPublish(msg)
	}

	return this.serverPublish(msg)
}

func (this *service) serverPublish(msg *message.PublishMessage) error {
	err := this.ctx.Topics.Subscribers(msg.Topic(), msg.QoS(), &this.subs, &this.qoss)
	if err != nil {
		return err
	}

	glog.Debugf("(%d) Publishing to %d clients", this.id, len(this.subs))

	for i, s := range this.subs {
		svc, ok := s.(*service)
		if !ok {
			//glog.Errorf("Invalid subscriber")
			glog.Errorf("Disconnecting client: %v", err)
			go svc.Disconnect()
		} else {
			//glog.Debugf("(%d) publishing to client %s with packetID %d qos %d", this.id, svc.cid, msg.PacketId(), qoss[i])
			msg.SetQoS(this.qoss[i])
			err := svc.Publish(msg, nil)
			if err == ErrBufferNotReady {
				glog.Errorf("Disconnecting client: %v", err)
				go svc.Disconnect()
			}
		}
	}

	return nil
}

func (this *service) clientPublish(msg *message.PublishMessage) error {
	err := this.ctx.Topics.Subscribers(msg.Topic(), msg.QoS(), &this.subs, &this.qoss)
	if err != nil {
		return err
	}

	//glog.Debugf("(%d) Found %d clients", this.id, len(subs))

	for _, s := range this.subs {
		if s != nil {
			fn, ok := s.(OnPublishFunc)
			if !ok {
				glog.Errorf("Invalid onPublish Function")
				return fmt.Errorf("Invalid onPublish Function")
			} else {
				//glog.Debugf("(%d) calling onpublish function", this.id)
				fn(msg)
			}
		}
	}

	return nil
}
