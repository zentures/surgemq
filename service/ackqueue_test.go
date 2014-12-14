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
	"testing"

	"github.com/dataence/assert"
	"github.com/surge/surgemq/message"
)

func TestAckQueueOutOfOrder(t *testing.T) {
	q := newAckqueue(5)
	assert.Equal(t, true, 8, q.cap())

	for i := 0; i < 12; i++ {
		msg := newPublishMessage(uint16(i), 1)
		q.wait(msg, nil)
	}

	assert.Equal(t, true, 12, q.len())

	ack1 := message.NewPubackMessage()
	ack1.SetPacketId(1)
	q.ack(ack1)

	acked := q.acked()

	assert.Equal(t, true, 0, len(acked))

	ack0 := message.NewPubackMessage()
	ack0.SetPacketId(0)
	q.ack(ack0)

	acked = q.acked()

	assert.Equal(t, true, 2, len(acked))
}
