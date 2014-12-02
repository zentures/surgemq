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
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/dataence/assert"
	"github.com/dataence/glog"
	"github.com/surge/surgemq/auth"
	"github.com/surge/surgemq/message"
)

func TestServiceConnectClientServer(t *testing.T) {
	runClientServerTests(t, nil)
}

func TestServiceConnectClientAuthError(t *testing.T) {
	var wg sync.WaitGroup

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})

	uri := "tcp://127.0.0.1:1883"
	u, err := url.Parse(uri)
	assert.NoError(t, true, err, "Error parsing URL")

	ctx := newTempContext()
	ctx.Auth = auth.MockFailureAuthenticator

	// Start listener
	wg.Add(1)
	go startService(t, u, ctx, &wg, ready1, ready2)

	<-ready1

	svc := connectToServer(t, uri, false)
	assert.Nil(t, true, svc)

	close(ready2)

	wg.Wait()
}

func TestServiceSubUnsubSuccess(t *testing.T) {

	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})

		sub := newSubscribeMessage(1)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				unsub := newUnsubscribeMessage()
				svc.Unsubscribe(unsub, func(msg, ack message.Message, err error) {
					close(done)
				})
			},
			func(msg *message.PublishMessage) error {
				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}
	})
}

// Subscribe with QoS 0, publish with QoS 0. So the client should receive all the
// messages as QoS 0.
func TestServiceSub0Pub0(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})

		count := uint16(0)

		sub := newSubscribeMessage(0)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				close(done)
			},
			func(msg *message.PublishMessage) error {
				assertPublishMessage(t, msg, count, 0)

				count++

				if count == 10 {
					glog.Debugf("got 10 pub0")
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}

		msg := newPublishMessage(0, 0)

		for i := uint16(0); i < 10; i++ {
			svc.Publish(msg, nil)
		}

		select {
		case <-done2:
			assert.Equal(t, true, 10, count)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for publish messages")
		}

	})
}

// Subscribe with QoS 1, publish with QoS 0. So the client should receive all the
// messages as QoS 0.
func TestServiceSub1Pub0(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})

		count := uint16(0)

		sub := newSubscribeMessage(1)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				close(done)
			},
			func(msg *message.PublishMessage) error {
				assertPublishMessage(t, msg, count, 0)

				count++

				if count == 10 {
					glog.Debugf("got 10 pub0")
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}

		msg := newPublishMessage(0, 0)

		for i := uint16(0); i < 10; i++ {
			svc.Publish(msg, nil)
		}

		select {
		case <-done2:
			assert.Equal(t, true, 10, count)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for publish messages")
		}

	})
}

// Subscribe w/ QoS 0, but publish as QoS 1. So the client should not receive any
// published messages.
func TestServiceSub0Pub1(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})

		ackcnt := 0

		sub := newSubscribeMessage(0)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				close(done)
			},
			func(msg *message.PublishMessage) error {
				assert.Fail(t, true, "Should not have received any publish message")
				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 1)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) {
					ackcnt++

					assert.NoError(t, true, err)

					pub, ok := msg.(*message.PublishMessage)
					assert.True(t, true, ok)

					puback, ok := ack.(*message.PubackMessage)
					assert.True(t, true, ok)

					assert.Equal(t, true, pub.PacketId(), puback.PacketId())

					if pub.PacketId() == 10 {
						close(done2)
					}
				})
		}

		select {
		case <-done2:
			assert.Equal(t, true, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for puback messages")
		}

		select {
		case <-time.After(time.Millisecond * 300):
		}
	})
}

// Subscribe with QoS 1, publish with QoS 1. So the client should receive all the
// messages as QoS 1.
func TestServiceSub1Pub1(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})
		done3 := make(chan struct{})

		count := uint16(0)
		ackcnt := 0

		sub := newSubscribeMessage(1)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				close(done)
			},
			func(msg *message.PublishMessage) error {
				count++

				assertPublishMessage(t, msg, count, 1)

				if count == 10 {
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 1)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) {
					ackcnt++

					assert.NoError(t, true, err)

					pub, ok := msg.(*message.PublishMessage)
					assert.True(t, true, ok)

					puback, ok := ack.(*message.PubackMessage)
					assert.True(t, true, ok)

					assert.Equal(t, true, pub.PacketId(), puback.PacketId())

					if pub.PacketId() == 10 {
						close(done3)
					}
				})
		}

		select {
		case <-done2:
			assert.Equal(t, true, 10, count)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for publish messages")
		}

		select {
		case <-done3:
			assert.Equal(t, true, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for puback messages")
		}
	})
}

// Subscribe with QoS 2, publish with QoS 1. So the client should receive all the
// messages as QoS 1.
func TestServiceSub2Pub1(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})
		done3 := make(chan struct{})

		count := uint16(0)
		ackcnt := 0

		sub := newSubscribeMessage(2)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				close(done)
			},
			func(msg *message.PublishMessage) error {
				count++

				assertPublishMessage(t, msg, count, 1)

				if count == 10 {
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 1)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) {
					ackcnt++

					assert.NoError(t, true, err)

					pub, ok := msg.(*message.PublishMessage)
					assert.True(t, true, ok)

					puback, ok := ack.(*message.PubackMessage)
					assert.True(t, true, ok)

					assert.Equal(t, true, pub.PacketId(), puback.PacketId())

					if pub.PacketId() == 10 {
						close(done3)
					}
				})
		}

		select {
		case <-done2:
			assert.Equal(t, true, 10, count)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for publish messages")
		}

		select {
		case <-done3:
			assert.Equal(t, true, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for puback messages")
		}
	})
}

// Subscribe w/ QoS 1, but publish as QoS 2. So the client should not receive any
// published messages.
func TestServiceSub1Pub2(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})

		ackcnt := 0

		sub := newSubscribeMessage(1)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				close(done)
			},
			func(msg *message.PublishMessage) error {
				assert.Fail(t, true, "Should not have received any publish message")
				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 2)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) {
					ackcnt++

					assert.NoError(t, true, err)

					pub, ok := msg.(*message.PublishMessage)
					assert.True(t, true, ok)

					pubcomp, ok := ack.(*message.PubcompMessage)
					assert.True(t, true, ok)

					assert.Equal(t, true, pub.PacketId(), pubcomp.PacketId())

					if pub.PacketId() == 10 {
						close(done2)
					}
				})
		}

		select {
		case <-done2:
			assert.Equal(t, true, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for puback messages")
		}

		select {
		case <-time.After(time.Millisecond * 300):
		}
	})
}

// Subscribe with QoS 2, publish with QoS 2. So the client should receive all the
// messages as QoS 2.
func TestServiceSub2Pub2(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})
		done3 := make(chan struct{})

		count := uint16(0)
		ackcnt := 0

		sub := newSubscribeMessage(2)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) {
				close(done)
			},
			func(msg *message.PublishMessage) error {
				count++

				assertPublishMessage(t, msg, count, 2)

				if count == 10 {
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 2)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) {
					ackcnt++

					assert.NoError(t, true, err)

					pub, ok := msg.(*message.PublishMessage)
					assert.True(t, true, ok)

					pubcomp, ok := ack.(*message.PubcompMessage)
					assert.True(t, true, ok)

					assert.Equal(t, true, pub.PacketId(), pubcomp.PacketId())

					if pub.PacketId() == 10 {
						close(done3)
					}
				})
		}

		select {
		case <-done2:
			assert.Equal(t, true, 10, count)

		case <-time.After(time.Millisecond * 300):
			assert.Fail(t, true, "Timed out waiting for publish messages")
		}

		select {
		case <-done3:
			assert.Equal(t, true, 10, ackcnt)

		case <-time.After(time.Millisecond * 600):
			assert.Fail(t, true, "Timed out waiting for puback messages")
		}
	})
}

func assertPublishMessage(t *testing.T, msg *message.PublishMessage, pktid uint16, qos byte) {
	assert.Equal(t, true, "abc", string(msg.Payload()))
	assert.Equal(t, true, qos, msg.QoS())

	if qos != 0 {
		assert.Equal(t, true, pktid, msg.PacketId())
	}
}
