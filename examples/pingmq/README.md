pingmq
======

[pingmq](https://github.com/surge/surgemq/tree/master/cmd/pingmq) is developed to demonstrate the different use cases one can use SurgeMQ. In this simplified use case, a network administrator can setup server uptime monitoring system by periodically sending ICMP ECHO_REQUEST to all the IPs in their network, and send the results to SurgeMQ.

Then multiple clients can subscribe to results based on their different needs. For example, a client maybe only interested in any failed ping attempts, as that would indicate a host might be down. After a certain number of failures the client may then raise some type of flag to indicate host down.

There are three benefits of using SurgeMQ for this use case. 

* First, with all the different monitoring tools out there that wants to know if hosts are up or down, they can all now subscribe to a single source of information. They no longer need to write their own uptime tools. 
* Second, assuming there are 5 monitoring tools on the network that wants to ping each and every host, the small packets are going to congest the network. The company can save 80% on their uptime monitoring bandwidth by having a single tool that pings the hosts, and have the rest subscribe to the results. 
* Third/last, the company can enhance their security posture by placing tighter restrictions on their firewalls if there's only a single host that can send ICMP ECHO_REQUESTS to all other hosts.

The following commands will run pingmq as a server, pinging the 8.8.8.0/28 CIDR block, and publishing the results to /ping/success/{ip} and /ping/failure/{ip} topics every 30 seconds. `sudo` is needed because we are using RAW sockets and that requires root privilege.

```
$ go build
$ sudo ./pingmq server -p 8.8.8.0/28 -i 30
```

The following command will run pingmq as a client, subscribing to /ping/failure/+ topic and receiving any failed ping attempts.

```
$ ./pingmq client -t /ping/failure/+
8.8.8.6: Request timed out for seq 1
```

The following command will run pingmq as a client, subscribing to /ping/failure/+ topic and receiving any failed ping attempts.

```
$ ./pingmq client -t /ping/success/+
8 bytes from 8.8.8.8: seq=1 ttl=56 tos=32 time=21.753711ms
```

One can also subscribe to a specific IP by using the following command.

```
$ ./pingmq client -t /ping/+/8.8.8.8
8 bytes from 8.8.8.8: seq=1 ttl=56 tos=32 time=21.753711ms
```