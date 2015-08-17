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
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/surge/glog"
	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/topics"
)

var authenticator string = "mockSuccess"

func TestServiceConnectSuccess(t *testing.T) {
	runClientServerTests(t, nil)
}

func TestServiceConnectAuthError(t *testing.T) {
	old := authenticator
	authenticator = "mockFailure"
	runClientServerTests(t, nil)
	authenticator = old
}

func TestServiceWillDelivery(t *testing.T) {
	var wg sync.WaitGroup

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})
	ready3 := make(chan struct{})
	subscribers := 3

	uri := "tcp://127.0.0.1:1883"
	u, err := url.Parse(uri)
	require.NoError(t, err, "Error parsing URL")

	// Start listener
	wg.Add(1)
	go startServiceN(t, u, &wg, ready1, ready2, subscribers)

	<-ready1

	c1 := connectToServer(t, uri)
	require.NotNil(t, c1)
	defer topics.Unregister(c1.svc.sess.ID())

	c2 := connectToServer(t, uri)
	require.NotNil(t, c2)
	defer topics.Unregister(c2.svc.sess.ID())

	c3 := connectToServer(t, uri)
	require.NotNil(t, c3)
	defer topics.Unregister(c3.svc.sess.ID())

	sub := message.NewSubscribeMessage()
	sub.AddTopic([]byte("will"), 1)

	subdone := int64(0)
	willdone := int64(0)

	c2.Subscribe(sub,
		func(msg, ack message.Message, err error) error {
			subs := atomic.AddInt64(&subdone, 1)
			if subs == int64(subscribers-1) {
				c1.Disconnect()
			}

			return nil
		},
		func(msg *message.PublishMessage) error {
			require.Equal(t, message.QosAtLeastOnce, msg.QoS())
			require.Equal(t, []byte("send me home"), msg.Payload())

			will := atomic.AddInt64(&willdone, 1)
			if will == int64(subscribers-1) {
				close(ready3)
			}

			return nil
		})

	c3.Subscribe(sub,
		func(msg, ack message.Message, err error) error {
			subs := atomic.AddInt64(&subdone, 1)
			if subs == int64(subscribers-1) {
				c1.Disconnect()
			}

			return nil
		},
		func(msg *message.PublishMessage) error {
			require.Equal(t, message.QosAtLeastOnce, msg.QoS())
			require.Equal(t, []byte("send me home"), msg.Payload())

			will := atomic.AddInt64(&willdone, 1)
			if will == int64(subscribers-1) {
				close(ready3)
			}

			return nil
		})

	select {
	case <-ready3:

	case <-time.After(time.Millisecond * 100):
		require.FailNow(t, "Test timed out")
	}

	c2.Disconnect()

	close(ready2)

	wg.Wait()
}

func TestServiceSubUnsub(t *testing.T) {
	runClientServerTests(t, func(c *Client) {
		done := make(chan struct{})

		sub := newSubscribeMessage(1)
		c.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				unsub := newUnsubscribeMessage()
				return c.Unsubscribe(unsub, func(msg, ack message.Message, err error) error {
					close(done)
					return nil
				})

			},
			func(msg *message.PublishMessage) error {
				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for subscribe response")
		}
	})
}

func TestServiceSubRetain(t *testing.T) {
	runClientServerTests(t, func(c *Client) {
		rmsg := message.NewPublishMessage()
		rmsg.SetRetain(true)
		rmsg.SetQoS(0)
		rmsg.SetTopic([]byte("abc"))
		rmsg.SetPayload([]byte("this is a test"))

		tmgr, _ := topics.NewManager("mem")
		err := tmgr.Retain(rmsg)
		require.NoError(t, err)

		done := make(chan struct{})

		sub := newSubscribeMessage(1)
		c.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				unsub := newUnsubscribeMessage()
				return c.Unsubscribe(unsub, func(msg, ack message.Message, err error) error {
					close(done)
					return nil
				})

			},
			func(msg *message.PublishMessage) error {
				require.Equal(t, msg.Topic(), []byte("abc"))
				require.Equal(t, msg.Payload(), []byte("this is a test"))
				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for subscribe response")
		}
	})
}

// Subscribe with QoS 0, publish with QoS 0. So the client should receive all the
// messages as QoS 0.
func TestServiceSub0Pub0(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})

		count := 0

		sub := newSubscribeMessage(0)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				close(done)
				return nil
			},
			func(msg *message.PublishMessage) error {
				assertPublishMessage(t, msg, 0)

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
			require.FailNow(t, "Timed out waiting for subscribe response")
		}

		msg := newPublishMessage(0, 0)

		for i := uint16(0); i < 10; i++ {
			svc.Publish(msg, nil)
		}

		select {
		case <-done2:
			require.Equal(t, 10, count)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for publish messages")
		}

	})
}

// Subscribe with QoS 1, publish with QoS 0. So the client should receive all the
// messages as QoS 0.
func TestServiceSub1Pub0(t *testing.T) {
	runClientServerTests(t, func(svc *Client) {
		done := make(chan struct{})
		done2 := make(chan struct{})

		count := 0

		sub := newSubscribeMessage(1)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				close(done)
				return nil
			},
			func(msg *message.PublishMessage) error {
				assertPublishMessage(t, msg, 0)

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
			require.FailNow(t, "Timed out waiting for subscribe response")
		}

		msg := newPublishMessage(0, 0)

		for i := uint16(0); i < 10; i++ {
			svc.Publish(msg, nil)
		}

		select {
		case <-done2:
			require.Equal(t, 10, count)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for publish messages")
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
			func(msg, ack message.Message, err error) error {
				close(done)
				return nil
			},
			func(msg *message.PublishMessage) error {
				require.FailNow(t, "Should not have received any publish message")
				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 1)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) error {
					ackcnt++

					require.NoError(t, err)

					pub, ok := msg.(*message.PublishMessage)
					require.True(t, ok)

					puback, ok := ack.(*message.PubackMessage)
					require.True(t, ok)

					require.Equal(t, pub.PacketId(), puback.PacketId())

					if pub.PacketId() == 10 {
						close(done2)
					}

					return nil
				})
		}

		select {
		case <-done2:
			require.Equal(t, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for puback messages")
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

		count := 0
		ackcnt := 0

		sub := newSubscribeMessage(1)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				close(done)
				return nil
			},
			func(msg *message.PublishMessage) error {
				count++

				assertPublishMessage(t, msg, 1)

				if count == 10 {
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 1)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) error {
					ackcnt++

					require.NoError(t, err)

					pub, ok := msg.(*message.PublishMessage)
					require.True(t, ok)

					puback, ok := ack.(*message.PubackMessage)
					require.True(t, ok)

					require.Equal(t, pub.PacketId(), puback.PacketId())

					if pub.PacketId() == 10 {
						close(done3)
					}

					return nil
				})
		}

		select {
		case <-done2:
			require.Equal(t, 10, count)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for publish messages")
		}

		select {
		case <-done3:
			require.Equal(t, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for puback messages")
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

		count := 0
		ackcnt := 0

		sub := newSubscribeMessage(2)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				close(done)
				return nil
			},
			func(msg *message.PublishMessage) error {
				count++

				assertPublishMessage(t, msg, 1)

				if count == 10 {
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 1)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) error {
					ackcnt++

					require.NoError(t, err)

					pub, ok := msg.(*message.PublishMessage)
					require.True(t, ok)

					puback, ok := ack.(*message.PubackMessage)
					require.True(t, ok)

					require.Equal(t, pub.PacketId(), puback.PacketId())

					if pub.PacketId() == 10 {
						close(done3)
					}

					return nil
				})
		}

		select {
		case <-done2:
			require.Equal(t, 10, count)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for publish messages")
		}

		select {
		case <-done3:
			require.Equal(t, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for puback messages")
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
			func(msg, ack message.Message, err error) error {
				close(done)
				return nil
			},
			func(msg *message.PublishMessage) error {
				require.FailNow(t, "Should not have received any publish message")
				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 2)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) error {
					ackcnt++

					require.NoError(t, err)

					pub, ok := msg.(*message.PublishMessage)
					require.True(t, ok)

					pubcomp, ok := ack.(*message.PubcompMessage)
					require.True(t, ok)

					require.Equal(t, pub.PacketId(), pubcomp.PacketId())

					if pub.PacketId() == 10 {
						close(done2)
					}

					return nil
				})
		}

		select {
		case <-done2:
			require.Equal(t, 10, ackcnt)

		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for puback messages")
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

		count := 0
		ackcnt := 0

		sub := newSubscribeMessage(2)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				close(done)
				return nil
			},
			func(msg *message.PublishMessage) error {
				count++

				assertPublishMessage(t, msg, 2)

				if count == 10 {
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Millisecond * 100):
			require.FailNow(t, "Timed out waiting for subscribe response")
		}

		for i := uint16(1); i <= 10; i++ {
			msg := newPublishMessage(i, 2)

			svc.Publish(msg,
				func(msg, ack message.Message, err error) error {
					ackcnt++

					require.NoError(t, err)

					pub, ok := msg.(*message.PublishMessage)
					require.True(t, ok)

					pubcomp, ok := ack.(*message.PubcompMessage)
					require.True(t, ok)

					require.Equal(t, pub.PacketId(), pubcomp.PacketId())

					if pub.PacketId() == 10 {
						close(done3)
					}

					return nil
				})
		}

		select {
		case <-done2:
			require.Equal(t, 10, count)

		case <-time.After(time.Millisecond * 300):
			require.FailNow(t, fmt.Sprintf("Timed out waiting for publish messages. Expecting %d, got %d.", 10, count))
		}

		select {
		case <-done3:
			require.Equal(t, 10, ackcnt)

		case <-time.After(time.Millisecond * 600):
			require.FailNow(t, "Timed out waiting for puback messages")
		}
	})
}

func assertPublishMessage(t *testing.T, msg *message.PublishMessage, qos byte) {
	require.Equal(t, "abc", string(msg.Payload()))
	require.Equal(t, qos, msg.QoS())
}
