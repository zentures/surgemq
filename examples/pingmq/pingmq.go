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

// pingmq is developed to demonstrate the different use cases one can use SurgeMQ.
// In this simplified use case, a network administrator can setup server uptime
// monitoring system by periodically sending ICMP ECHO_REQUEST to all the IPs
// in their network, and send the results to SurgeMQ.
//
// Then multiple clients can subscribe to results based on their different needs.
// For example, a client maybe only interested in any failed ping attempts, as that
// would indicate a host might be down. After a certain number of failures the
// client may then raise some type of flag to indicate host down.
//
// There are three benefits of using SurgeMQ for this use case. First, with all the
// different monitoring tools out there that wants to know if hosts are up or down,
// they can all now subscribe to a single source of information. They no longer
// need to write their own uptime tools. Second, assuming there are 5 monitoring
// tools on the network that wants to ping each and every host, the small packets
// are going to congest the network. The company can save 80% on their uptime
// monitoring bandwidth by having a single tool that pings the hosts, and have the
// rest subscribe to the results. Third/last, the company can enhance their security
// posture by placing tighter restrictions on their firewalls if there's only a
// single host that can send ICMP ECHO_REQUESTS to all other hosts.
//
// The following commands will run pingmq as a server, pinging the 8.8.8.0/28 CIDR
// block, and publishing the results to /ping/success/{ip} and /ping/failure/{ip}
// topics every 30 seconds. `sudo` is needed because we are using RAW sockets and
// that requires root privilege.
//
//   $ go build
//   $ sudo ./pingmq server -p 8.8.8.0/28 -i 30
//
// The following command will run pingmq as a client, subscribing to /ping/failure/+
// topic and receiving any failed ping attempts.
//
//   $ ./pingmq client -t /ping/failure/+
//   8.8.8.6: Request timed out for seq 1
//
// The following command will run pingmq as a client, subscribing to /ping/failure/+
// topic and receiving any failed ping attempts.
//
//   $ ./pingmq client -t /ping/success/+
//   8 bytes from 8.8.8.8: seq=1 ttl=56 tos=32 time=21.753711ms
//
// One can also subscribe to a specific IP by using the following command.
//
//   $ ./pingmq client -t /ping/+/8.8.8.8
//   8 bytes from 8.8.8.8: seq=1 ttl=56 tos=32 time=21.753711ms
//
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/surge/netx"
	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/service"
)

type strlist []string

func (this *strlist) String() string {
	return fmt.Sprint(*this)
}

func (this *strlist) Type() string {
	return "strlist"
}

func (this *strlist) Set(value string) error {
	for _, ip := range strings.Split(value, ",") {
		*this = append(*this, ip)
	}

	return nil
}

var (
	pingmqCmd = &cobra.Command{
		Use:   "pingmq",
		Short: "Pingmq is a program designed to demonstrate the SurgeMQ usage.",
		Long: `Pingmq demonstrates the use of SurgeMQ by pinging a list of hosts, 
publishing the result to any clients subscribed to two topics:
/ping/success/{ip} and /ping/failure/{ip}.`,
	}

	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "server starts a SurgeMQ server and publishes to it all the ping results",
	}

	clientCmd = &cobra.Command{
		Use:   "client",
		Short: "client subscribes to the pingmq server and prints out the ping results",
	}

	serverURI   string
	serverQuiet bool
	serverIPs   strlist

	pingInterval int

	clientURI    string
	clientTopics strlist

	s *service.Server
	c *service.Client
	p *netx.Pinger

	wg sync.WaitGroup

	done chan struct{}
)

func init() {
	serverCmd.Flags().StringVarP(&serverURI, "uri", "u", "tcp://:5836", "URI to run the server on")
	serverCmd.Flags().BoolVarP(&serverQuiet, "quiet", "q", false, "print out ping results")
	serverCmd.Flags().VarP(&serverIPs, "ping", "p", "Comma separated list of IPv4 addresses to ping")
	serverCmd.Flags().IntVarP(&pingInterval, "interval", "i", 60, "ping interval in seconds")
	serverCmd.Run = server

	clientCmd.Flags().StringVarP(&clientURI, "server", "s", "tcp://127.0.0.1:5836", "PingMQ server to connect to")
	clientCmd.Flags().VarP(&clientTopics, "topic", "t", "Comma separated list of topics to subscribe to")
	clientCmd.Run = client

	pingmqCmd.AddCommand(serverCmd)
	pingmqCmd.AddCommand(clientCmd)

	done = make(chan struct{})
}

func pinger() {
	p = &netx.Pinger{}
	if err := p.AddIPs(serverIPs); err != nil {
		log.Fatal(err)
	}

	cnt := 0
	tick := time.NewTicker(time.Duration(pingInterval) * time.Second)

	for {
		if cnt != 0 {
			<-tick.C
		}

		res, err := p.Start()
		if err != nil {
			log.Fatal(err)
		}

		for pr := range res {
			if !serverQuiet {
				log.Println(pr)
			}

			// Creates a new PUBLISH message with the appropriate contents for publishing
			pubmsg := message.NewPublishMessage()
			if pr.Err != nil {
				pubmsg.SetTopic([]byte(fmt.Sprintf("/ping/failure/%s", pr.Src)))
			} else {
				pubmsg.SetTopic([]byte(fmt.Sprintf("/ping/success/%s", pr.Src)))
			}
			pubmsg.SetQoS(0)

			b, err := pr.GobEncode()
			if err != nil {
				log.Printf("pinger: Error from GobEncode: %v\n", err)
				continue
			}

			pubmsg.SetPayload(b)

			// Publishes to the server
			s.Publish(pubmsg, nil)
		}

		p.Stop()
		cnt++
	}
}

func server(cmd *cobra.Command, args []string) {
	// Create a new server
	s = &service.Server{
		KeepAlive:        300,           // seconds
		ConnectTimeout:   2,             // seconds
		SessionsProvider: "mem",         // keeps sessions in memory
		Authenticator:    "mockSuccess", // always succeed
		TopicsProvider:   "mem",         // keeps topic subscriptions in memory
	}

	log.Printf("Starting server...")
	go func() {
		// Listen and serve connections at serverURI
		if err := s.ListenAndServe(serverURI); err != nil {
			log.Fatal(err)
		}
	}()
	time.Sleep(300 * time.Millisecond)

	log.Printf("Starting pinger...")
	pinger()
}

func client(cmd *cobra.Command, args []string) {
	// Instantiates a new Client
	c = &service.Client{}

	// Creates a new MQTT CONNECT message and sets the proper parameters
	msg := message.NewConnectMessage()
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte(fmt.Sprintf("pingmqclient%d%d", os.Getpid(), time.Now().Unix())))
	msg.SetKeepAlive(300)

	// Connects to the remote server at 127.0.0.1 port 1883
	if err := c.Connect(clientURI, msg); err != nil {
		log.Fatal(err)
	}

	// Creates a new SUBSCRIBE message to subscribe to topic "abc"
	submsg := message.NewSubscribeMessage()

	for _, t := range clientTopics {
		submsg.AddTopic([]byte(t), 0)
	}

	c.Subscribe(submsg, nil, onPublish)

	<-done
}

func onPublish(msg *message.PublishMessage) error {
	pr := &netx.PingResult{}
	if err := pr.GobDecode(msg.Payload()); err != nil {
		log.Printf("Error decoding ping result: %v\n", err)
		return err
	}

	log.Println(pr)
	return nil
}

func main() {
	pingmqCmd.Execute()
}
