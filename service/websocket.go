package service

import (
	"bytes"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// websocketConn wraps a websocket.Conn to satisfy the net.Conn and
// io.ReadWriteCloser interfaces
type websocketConn struct {
	buf        *bytes.Buffer
	readMutex  sync.Mutex
	writeMutex sync.Mutex
	*websocket.Conn
}

func (w *websocketConn) Read(p []byte) (n int, err error) {
	// If the buffer is empty, fill it from the socket
	if w.buf.Len() == 0 {
		w.readMutex.Lock()
		_, msg, err := w.ReadMessage()
		w.readMutex.Unlock()
		if err != nil {
			return 0, err
		}
		if _, err = w.buf.Write(msg); err != nil {
			return 0, err
		}
	}
	// Read bytes from the buffer
	return w.buf.Read(p)
}

func (w *websocketConn) Write(p []byte) (n int, err error) {
	w.writeMutex.Lock()
	err = w.WriteMessage(websocket.BinaryMessage, p)
	w.writeMutex.Unlock()
	return len(p), err
}

func (w *websocketConn) SetReadDeadline(t time.Time) (err error) {
	w.readMutex.Lock()
	err = w.Conn.SetReadDeadline(t)
	w.readMutex.Unlock()
	return err
}

func (w *websocketConn) SetWriteDeadline(t time.Time) (err error) {
	w.writeMutex.Lock()
	err = w.Conn.SetReadDeadline(t)
	w.writeMutex.Unlock()
	return err
}

func (w *websocketConn) SetDeadline(t time.Time) error {
	if err := w.SetReadDeadline(t); err != nil {
		return err
	}
	if err := w.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// newWebsocketConn wraps the provided websocket.Conn and returns a new
// websocketConn instance
func newWebsocketConn(ws *websocket.Conn) *websocketConn {
	return &websocketConn{
		buf:  bytes.NewBuffer(nil),
		Conn: ws,
	}
}
