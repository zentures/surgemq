Package message is an encoder/decoder library for MQTT 3.1 and 3.1.1 messages. You can
find the MQTT specs at the following locations:

>	3.1.1 - http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/
>	3.1 - http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html

From the spec:

>	MQTT is a Client Server publish/subscribe messaging transport protocol. It is
>	light weight, open, simple, and designed so as to be easy to implement. These
>	characteristics make it ideal for use in many situations, including constrained
>	environments such as for communication in Machine to Machine (M2M) and Internet
>	of Things (IoT) contexts where a small code footprint is required and/or network
>	bandwidth is at a premium.
>
>	The MQTT protocol works by exchanging a series of MQTT messages in a defined way.
>	The protocol runs over TCP/IP, or over other network protocols that provide
>	ordered, lossless, bi-directional connections.


There are two main items to take note in this package. The first is

```
	type MessageType byte
```

MessageType is the type representing the MQTT packet types. In the MQTT spec, MQTT
control packet type is represented as a 4-bit unsigned value. MessageType receives
several methods that returns string representations of the names and descriptions.

Also, one of the methods is New(). It returns a new Message object based on the mtype
parameter. For example:

```
	m, err := CONNECT.New()
	msg := m.(*ConnectMessage)
```

This would return a PublishMessage struct, but mapped to the Message interface. You can
then type assert it back to a *PublishMessage. Another way to create a new
PublishMessage is to call

```
	msg := NewConnectMessage()
```

Every message type has a New function that returns a new message. The list of available
message types are defined as constants below.

As you may have noticed, the second important item is the Message interface. It defines
several methods that are common to all messages, including Name(), Desc(), and Type().
Most importantly, it also defines the Encode() and Decode() methods.

```
	Encode() (io.Reader, int, error)
	Decode(io.Reader) (int, error)
```

Encode returns an io.Reader in which the encoded bytes can be read. The second return
value is the number of bytes encoded, so the caller knows how many bytes there will be.
If Encode returns an error, then the first two return values should be considered invalid.
Any changes to the message after Encode() is called will invalidate the io.Reader.

Decode reads from the io.Reader parameter until a full message is decoded, or when io.Reader
returns EOF or error. The first return value is the number of bytes read from io.Reader.
The second is error if Decode encounters any problems.

With these in mind, we can now do:

```
	// Create a new CONNECT message
	msg := NewConnectMessage()

	// Set the appropriate parameters
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte("surgemq"))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte("surgemq"))
	msg.SetPassword([]byte("verysecret"))

	// Encode the message and get the io.Reader
	r, n, err := msg.Encode()
	if err == nil {
		return err
	}

	// Write n bytes into the connection
	m, err := io.CopyN(conn, r, int64(n))
	if err != nil {
		return err
	}

	fmt.Printf("Sent %d bytes of %s message", m, msg.Name())
```

To receive a CONNECT message from a connection, we can do:

```
	// Create a new CONNECT message
	msg := NewConnectMessage()

	// Decode the message by reading from conn
	n, err := msg.Decode(conn)
```

If you don't know what type of message is coming down the pipe, you can do something like this:

```
	// Create a buffered IO reader for the connection
	br := bufio.NewReader(conn)

	// Peek at the first byte, which contains the message type
	b, err := br.Peek(1)
	if err != nil {
		return err
	}

	// Extract the type from the first byte
	t := MessageType(b[0] >> 4)

	// Create a new message
	msg, err := t.New()
	if err != nil {
		return err
	}

	// Decode it from the bufio.Reader
	n, err := msg.Decode(br)
	if err != nil {
		return err
	}
```
