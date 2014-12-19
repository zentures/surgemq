/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

// This is originally from
// git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/samples/simple.go
// I turned it into a test that I can run from `go test`
package service

import (
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"github.com/dataence/assert"
)

var f MQTT.MessageHandler = func(client *MQTT.MqttClient, msg MQTT.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func TestPahoGoClient(t *testing.T) {
	var wg sync.WaitGroup

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})

	uri := "tcp://127.0.0.1:1883"
	u, err := url.Parse(uri)
	assert.NoError(t, true, err, "Error parsing URL")

	// Start listener
	wg.Add(1)
	go startServiceN(t, u, &wg, ready1, ready2, 1)

	<-ready1

	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883").SetClientId("trivial")
	opts.SetDefaultPublishHandler(f)

	c := MQTT.NewClient(opts)
	_, err = c.Start()
	assert.NoError(t, true, err)

	filter, _ := MQTT.NewTopicFilter("/go-mqtt/sample", 0)
	receipt, err := c.StartSubscription(nil, filter)
	assert.NoError(t, true, err)

	select {
	case <-receipt:

	case <-time.After(time.Millisecond * 100):
		assert.Fail(t, true, "Test timed out")
	}

	for i := 0; i < 100; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		receipt := c.Publish(MQTT.QOS_ONE, "/go-mqtt/sample", []byte(text))

		select {
		case <-receipt:

		case <-time.After(time.Millisecond * 100):
			assert.Fail(t, true, "Test timed out")
		}
	}

	time.Sleep(3 * time.Second)

	receipt, err = c.EndSubscription("/go-mqtt/sample")
	assert.NoError(t, true, err)

	select {
	case <-receipt:

	case <-time.After(time.Millisecond * 100):
		assert.Fail(t, true, "Test timed out")
	}

	c.Disconnect(250)

	close(ready2)

	wg.Wait()
}
