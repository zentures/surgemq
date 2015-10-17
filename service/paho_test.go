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
	"github.com/stretchr/testify/require"
)

var f MQTT.MessageHandler = func(client *MQTT.Client, msg MQTT.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func TestPahoGoClient(t *testing.T) {
	var wg sync.WaitGroup

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})

	uri := "tcp://127.0.0.1:1883"
	u, err := url.Parse(uri)
	require.NoError(t, err, "Error parsing URL")

	// Start listener
	wg.Add(1)
	go startServiceN(t, u, &wg, ready1, ready2, 1)

	<-ready1

	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883").SetClientID("trivial")
	opts.SetDefaultPublishHandler(f)

	c := MQTT.NewClient(opts)
	token := c.Connect()
	token.Wait()
	require.NoError(t, token.Error())

	filters := map[string]byte{"/go-mqtt/sample": 0}
	token = c.SubscribeMultiple(filters, nil)
	token.WaitTimeout(time.Millisecond * 100)
	token.Wait()
	require.NoError(t, token.Error())

	for i := 0; i < 100; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token = c.Publish("/go-mqtt/sample", 1, false, []byte(text))
		token.WaitTimeout(time.Millisecond * 100)
		token.Wait()
		require.NoError(t, token.Error())
	}

	time.Sleep(3 * time.Second)

	token = c.Unsubscribe("/go-mqtt/sample")
	token.Wait()
	token.WaitTimeout(time.Millisecond * 100)
	require.NoError(t, token.Error())

	c.Disconnect(250)

	close(ready2)

	wg.Wait()
}
