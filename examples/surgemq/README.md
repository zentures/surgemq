# SurgeMQ Standalone Server

Standalone SurgeMQ server, creates listeners for plaintext MQTT, Websocket and Secure Websocket. Without any options, surgemq listens on port 1883 for plaintext MQTT.

## Build

* `go get github.com/surgemq/surgemq`
* `cd $GOPATH/src/github.com/surgemq/surgemq/examples/surgemq/`
* `go build`

## Usage

### Command line options

- `-help` : Shows complete list of supported options
- `-auth string`: Authenticator Type (default "mockSuccess")
- `-keepalive int`: Keepalive (sec) (default 300)
- `-sessions string`: Session Provider Type (default "mem")
- `-topics string`: Topics Provider Type (default "mem")
- `-wsaddr string`: HTTP websocket listener address, (eg. ":8080") (default none)
- `-wssaddr string`: HTTPS websocket listener address, (eg. ":8443") (default none)
- `-wsscertpath string`: HTTPS listener public key file, (eg. "certificate.pem") (default none)
- `-wsskeypath string`: HTTPS listener private key file, (eg. "key.pem") (default none)

## Websocket listener

1. In addition to listening for MQTT traffic on port 1883, the standalone server can be configured to listen for websocket over HTTP or HTTPS.
2. `surgemq -wsaddr :8080` will start the server to listen for Websocket on port 8080

## Self-signed Websocket listener

The following steps will setup the server to use a self-signed certificate.

1. Generate a self-signed TLS certificate:
`openssl genrsa -out key.pem 2048; openssl req -new -key key.pem -out csr.pem; openssl req -x509 -days 365 -key key.pem -in csr.pem -out certificate.pem`

2. Start standalone server: `surgemq.exe -wssaddr :8443 -wsscertpath certificate.pem -wsskeypath key.pem`

3. For self-signed certificate, add a security exception to the browser (eg: http://www.poweradmin.com/help/sslhints/firefox.aspx)

## Testing

Websocket support has been tested with the HiveMQ websocket client at http://www.hivemq.com/demos/websocket-client/
