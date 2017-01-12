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

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"

	"github.com/surge/glog"
	"github.com/surgemq/surgemq/service"
)

type stringSlice []string

var (
	keepAlive        int
	connectTimeout   int
	ackTimeout       int
	timeoutRetries   int
	authenticator    string
	sessionsProvider string
	topicsProvider   string
	cpuprofile       string
	wsAddr           string      // HTTPS websocket address eg. :8080
	wssAddr          string      // HTTPS websocket address, eg. :8081
	wssCertPath      string      // path to HTTPS public key
	wssKeyPath       string      // path to HTTPS private key
	wsOrigin         stringSlice // Allowed Origin(s)
)

// String returns a string representation
func (s *stringSlice) String() string {
	return fmt.Sprint(*s)
}

// Set appends to the slice from a comma-separated string value
func (s *stringSlice) Set(value string) error {
	for _, val := range strings.Split(value, `,`) {
		*s = append(*s, val)
	}

	return nil
}

func init() {
	flag.IntVar(&keepAlive, "keepalive", service.DefaultKeepAlive, "Keepalive (sec)")
	flag.IntVar(&connectTimeout, "connecttimeout", service.DefaultConnectTimeout, "Connect Timeout (sec)")
	flag.IntVar(&ackTimeout, "acktimeout", service.DefaultAckTimeout, "Ack Timeout (sec)")
	flag.IntVar(&timeoutRetries, "retries", service.DefaultTimeoutRetries, "Timeout Retries")
	flag.StringVar(&authenticator, "auth", service.DefaultAuthenticator, "Authenticator Type")
	flag.StringVar(&sessionsProvider, "sessions", service.DefaultSessionsProvider, "Session Provider Type")
	flag.StringVar(&topicsProvider, "topics", service.DefaultTopicsProvider, "Topics Provider Type")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "CPU Profile Filename")
	flag.StringVar(&wsAddr, "wsaddr", "", "HTTP websocket address, eg. ':8080'")
	flag.StringVar(&wssAddr, "wssaddr", "", "HTTPS websocket address, eg. ':8081'")
	flag.StringVar(&wssCertPath, "wsscertpath", "", "HTTPS server public key file")
	flag.StringVar(&wssKeyPath, "wsskeypath", "", "HTTPS server private key file")
	flag.Var(&wsOrigin, "wsorigin", "Allowed websocket Origin(s) (comma-separated), eg. 'http://localhost:8000,https://localhost:8081'")
	flag.Parse()
}

func main() {
	svr := &service.Server{
		KeepAlive:        keepAlive,
		ConnectTimeout:   connectTimeout,
		AckTimeout:       ackTimeout,
		TimeoutRetries:   timeoutRetries,
		SessionsProvider: sessionsProvider,
		TopicsProvider:   topicsProvider,
	}

	var f *os.File
	var err error

	if cpuprofile != "" {
		f, err = os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}

		pprof.StartCPUProfile(f)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, os.Kill)
	go func() {
		sig := <-sigchan
		glog.Errorf("Existing due to trapped signal; %v", sig)

		if f != nil {
			glog.Errorf("Stopping profile")
			pprof.StopCPUProfile()
			f.Close()
		}

		svr.Close()

		os.Exit(0)
	}()

	mqttaddr := "tcp://:1883"

	if len(wsAddr) > 0 || len(wssAddr) > 0 {
		if len(wsOrigin) > 0 {
			svr.WebsocketUpgrader = service.NewWebsocketUpgrader()
			svr.WebsocketUpgrader.CheckOrigin = wsOriginFunc
		}
		http.HandleFunc(`/mqtt`, svr.WebsocketHandler)
		/* start a plain websocket listener */
		if len(wsAddr) > 0 {
			go func() {
				if err = http.ListenAndServe(wsAddr, nil); err != nil {
					panic(err)
				}
			}()
		}
		/* start a secure websocket listener */
		if len(wssAddr) > 0 && len(wssCertPath) > 0 && len(wssKeyPath) > 0 {
			go func() {
				if err = http.ListenAndServeTLS(wssAddr, wssCertPath, wssKeyPath, nil); err != nil {
					panic(err)
				}
			}()
		}
	}

	/* create plain MQTT listener */
	err = svr.ListenAndServe(mqttaddr)
	if err != nil {
		glog.Errorf("surgemq/main: %v", err)
	}
}

func wsOriginFunc(r *http.Request) bool {
	for _, origin := range wsOrigin {
		if r.Header.Get(`Origin`) == origin {
			return true
		}
	}
	return false
}
