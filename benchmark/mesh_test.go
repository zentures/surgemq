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

package benchmark

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/surge/glog"
	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/service"
)

// Usage: go test -run=Mesh
func TestMesh(t *testing.T) {
	var wg sync.WaitGroup

	totalSent = 0
	totalRcvd = 0
	totalSentTime = 0
	totalRcvdTime = 0
	sentSince = 0
	rcvdSince = 0

	subdone = 0
	rcvdone = 0

	done = make(chan struct{})

	for i := 1; i < publishers+1; i++ {
		time.Sleep(time.Millisecond * 20)
		wg.Add(1)
		go startMeshClient(t, i, &wg)
	}

	wg.Wait()

	glog.Infof("Total Sent %d messages in %d ns, %d ns/msg, %d msgs/sec", totalSent, sentSince, int(float64(sentSince)/float64(totalSent)), int(float64(totalSent)/(float64(sentSince)/float64(time.Second))))
	glog.Infof("Total Received %d messages in %d ns, %d ns/msg, %d msgs/sec", totalRcvd, rcvdSince, int(float64(rcvdSince)/float64(totalRcvd)), int(float64(totalRcvd)/(float64(rcvdSince)/float64(time.Second))))
}

func startMeshClient(t testing.TB, cid int, wg *sync.WaitGroup) {
	runClientTest(t, cid, wg, func(svc *service.Client) {
		done2 := make(chan struct{})

		cnt := messages
		expected := publishers * cnt

		received := 0
		sent := 0

		now := time.Now()
		since := time.Since(now).Nanoseconds()

		sub := newSubscribeMessage("test", 0)
		svc.Subscribe(sub,
			func(msg, ack message.Message, err error) error {
				subs := atomic.AddInt64(&subdone, 1)
				if subs == int64(publishers) {
					close(done)
				}
				return nil
			},
			func(msg *message.PublishMessage) error {
				if received == 0 {
					now = time.Now()
				}

				received++
				//glog.Debugf("(surgemq%d) messages received=%d", cid, received)
				since = time.Since(now).Nanoseconds()

				if received == expected {
					close(done2)
				}

				return nil
			})

		select {
		case <-done:
		case <-time.After(time.Second * time.Duration(publishers)):
			glog.Infof("(surgemq%d) Timed out waiting for subscribe response", cid)
			return
		}

		payload := make([]byte, size)
		msg := message.NewPublishMessage()
		msg.SetTopic(topic)
		msg.SetQoS(qos)

		go func() {
			now := time.Now()

			for i := 0; i < cnt; i++ {
				binary.BigEndian.PutUint32(payload, uint32(cid*cnt+i))
				msg.SetPayload(payload)

				err := svc.Publish(msg, nil)
				if err != nil {
					break
				}
				sent++
			}

			since := time.Since(now).Nanoseconds()

			statMu.Lock()
			totalSent += int64(sent)
			totalSentTime += int64(since)
			if since > sentSince {
				sentSince = since
			}
			statMu.Unlock()

			glog.Debugf("(surgemq%d) Sent %d messages in %d ns, %d ns/msg, %d msgs/sec", cid, sent, since, int(float64(since)/float64(cnt)), int(float64(sent)/(float64(since)/float64(time.Second))))
		}()

		select {
		case <-done2:
		case <-time.Tick(time.Second * time.Duration(nap*publishers)):
			glog.Errorf("Timed out waiting for messages to be received.")
		}

		statMu.Lock()
		totalRcvd += int64(received)
		totalRcvdTime += int64(since)
		if since > rcvdSince {
			rcvdSince = since
		}
		statMu.Unlock()

		glog.Debugf("(surgemq%d) Received %d messages in %d ns, %d ns/msg, %d msgs/sec", cid, received, since, int(float64(since)/float64(cnt)), int(float64(received)/(float64(since)/float64(time.Second))))
	})
}
