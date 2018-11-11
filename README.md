SurgeMQ
=======

[![GoDoc](http://godoc.org/github.com/surgemq/surgemq?status.svg)](http://godoc.org/github.com/surgemq/surgemq)

SurgeMQ is a high performance MQTT broker and client library that aims to be fully compliant with MQTT 3.1 and 3.1.1 specs. The primary package that's of interest is package [service](http://godoc.org/github.com/surgemq/surgemq/service). It provides the MQTT Server and Client services in a library form.

SurgeMQ development is currently **on hold and unmaintained**. 

**This project should be considered unstable until further notice.**

### MQTT

According to the [MQTT spec](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html):

> MQTT is a Client Server publish/subscribe messaging transport protocol. It is light weight, open, simple, and designed so as to be easy to implement. These characteristics make it ideal for use in many situations, including constrained environments such as for communication in Machine to Machine (M2M) and Internet of Things (IoT) contexts where a small code footprint is required and/or network bandwidth is at a premium.
>
> The protocol runs over TCP/IP, or over other network protocols that provide ordered, lossless, bi-directional connections. Its features include:
> 
> * Use of the publish/subscribe message pattern which provides one-to-many message distribution and decoupling of applications.
> * A messaging transport that is agnostic to the content of the payload.
> * Three qualities of service for message delivery:
>   * "At most once", where messages are delivered according to the best efforts of the operating environment. Message loss can occur. This level could be used, for example, with ambient sensor data where it does not matter if an individual reading is lost as the next one will be published soon after.
>   * "At least once", where messages are assured to arrive but duplicates can occur.
>   * "Exactly once", where message are assured to arrive exactly once. This level could be used, for example, with billing systems where duplicate or lost messages could lead to incorrect charges being applied.
> * A small transport overhead and protocol exchanges minimized to reduce network traffic.
> * A mechanism to notify interested parties when an abnormal disconnection occurs.

There's some very large implementation of MQTT such as [Facebook Messenger](https://www.facebook.com/notes/facebook-engineering/building-facebook-messenger/10150259350998920). There's also an active Eclipse project, [Paho](https://eclipse.org/paho/), that provides scalable open-source client implementations for many different languages, including C/C++, Java, Python, JavaScript, C# .Net and Go.

### Features, Limitations, and Future

**Features**

* Supports QOS 0, 1 and 2 messages
* Supports will messages
* Supports retained messages (add/remove)
* Pretty much everything in the spec except for the list below

**Limitations**

* All features supported are in memory only. Once the server restarts everything is cleared.
  * However, all the components are written to be pluggable so one can write plugins based on the Go interfaces defined.
* Message redelivery on reconnect is not currently supported.
* Message offline queueing on disconnect is not supported. Though this is also not a specific requirement for MQTT.

**Future**

* Message re-delivery (DUP)
* $SYS topics
* Server bridge
* Ack timeout/retry
* Session persistence
* Better authentication modules

### Performance

Current performance benchmark of SurgeMQ, running all publishers, subscribers and broker on a single 4-core (2.8Ghz i7) MacBook Pro,is able to achieve:

* over **400,000 MPS** in a 1:1 single publisher and single producer configuration
* over **450,000 MPS** in a 20:1 fan-in configuration
* over **750,000 MPS** in a 1:20 fan-out configuration
* over **700,000 MPS** in a full mesh configuration with 20 clients

### Compatibility

In addition, SurgeMQ has been tested with the following client libraries and it _seems_ to work:

* libmosquitto 1.3.5 (in C)
  * Tested with the bundled test programs msgsps_pub and msgsps_sub
* Paho MQTT Conformance/Interoperability Testing Suite (in Python). Tested with all 10 test cases, 3 did not pass. They are 
  1. "offline messages queueing test" which is not supported by SurgeMQ
  2. "redelivery on reconnect test" which is not yet implemented by SurgeMQ
  3. "run subscribe failure test" which is not a valid test
* Paho Go Client Library (in Go)
  * Tested with one of the tests in the library, in fact, that tests is now part of the tests for SurgeMQ
* Paho C Client library (in C)
  * Tested with most of the test cases and failed the same ones as the conformance test because the features are not yet implemented.
  * Actually I think there's a bug in the test suite as it calls the PUBLISH handler function for non-PUBLISH messages.

### Documentation

Documentation is available at [godoc](http://godoc.org/github.com/surgemq/surgemq).

More information regarding the design of the SurgeMQ is available at [zen 3.1](http://surgemq.com).

### License

Copyright (c) 2014 Dataence, LLC. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


### Examples

#### PingMQ

`pingmq` is developed to demonstrate the different use cases one can use SurgeMQ. In this simplified use case, a network administrator can setup server uptime monitoring system by periodically sending ICMP ECHO_REQUEST to all the IPs in their network, and send the results to SurgeMQ.

Then multiple clients can subscribe to results based on their different needs. For example, a client maybe only interested in any failed ping attempts, as that would indicate a host might be down. After a certain number of failures the client may then raise some type of flag to indicate host down.

`pingmq` is available [here](https://github.com/surgemq/surgemq/tree/master/cmd/pingmq) and documentation is available at [godoc](http://godoc.org/github.com/surgemq/surgemq/cmd/pingmq). It utilizes [surge/ping](https://github.com/surge/ping) to perform the pings.

#### Server Example

```
// Create a new server
svr := &service.Server{
    KeepAlive:        300,               // seconds
    ConnectTimeout:   2,                 // seconds
    SessionsProvider: "mem",             // keeps sessions in memory
    Authenticator:    "mockSuccess",     // always succeed
    TopicsProvider:   "mem",             // keeps topic subscriptions in memory
}

// Listen and serve connections at localhost:1883
svr.ListenAndServe("tcp://:1883")
```
#### Client Example

```
// Instantiates a new Client
c := &Client{}

// Creates a new MQTT CONNECT message and sets the proper parameters
msg := message.NewConnectMessage()
msg.SetWillQos(1)
msg.SetVersion(4)
msg.SetCleanSession(true)
msg.SetClientId([]byte("surgemq"))
msg.SetKeepAlive(10)
msg.SetWillTopic([]byte("will"))
msg.SetWillMessage([]byte("send me home"))
msg.SetUsername([]byte("surgemq"))
msg.SetPassword([]byte("verysecret"))

// Connects to the remote server at 127.0.0.1 port 1883
c.Connect("tcp://127.0.0.1:1883", msg)

// Creates a new SUBSCRIBE message to subscribe to topic "abc"
submsg := message.NewSubscribeMessage()
submsg.AddTopic([]byte("abc"), 0)

// Subscribes to the topic by sending the message. The first nil in the function
// call is a OnCompleteFunc that should handle the SUBACK message from the server.
// Nil means we are ignoring the SUBACK messages. The second nil should be a
// OnPublishFunc that handles any messages send to the client because of this
// subscription. Nil means we are ignoring any PUBLISH messages for this topic.
c.Subscribe(submsg, nil, nil)

// Creates a new PUBLISH message with the appropriate contents for publishing
pubmsg := message.NewPublishMessage()
pubmsg.SetPacketId(pktid)
pubmsg.SetTopic([]byte("abc"))
pubmsg.SetPayload(make([]byte, 1024))
pubmsg.SetQoS(qos)

// Publishes to the server by sending the message
c.Publish(pubmsg, nil)

// Disconnects from the server
c.Disconnect()
```

