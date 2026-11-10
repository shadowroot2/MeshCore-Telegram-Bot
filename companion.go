package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
	"unicode/utf8"
)

// Wire format matches zephcore/app/CompanionProtocol.h and Arduino MeshCore:
// app->node '<' + LE16 len + payload, node->app '>' + LE16 len + payload.
const (
	cmdAppStart          = 0x01
	cmdSendChannelTxtMsg = 0x03
	cmdSyncNextMessage   = 0x0A
	cmdGetChannel        = 0x1F
	cmdRunCLICommand     = 0x42

	pktOK           = 0x00
	pktError        = 0x01
	pktContactMsgV2 = 0x07
	pktChannelMsgV2 = 0x08
	pktNoMoreMsgs   = 0x0A
	pktSelfInfo     = 0x05
	pktContactMsgV3 = 0x10
	pktChannelMsgV3 = 0x11
	pktChannelInfo  = 0x12
	pktChannelData  = 0x1B
	pktCliReply     = 0x1D

	pushMsgWaiting = 0x83

	txtTypePlain     = 0
	maxFrameLen      = 2048
	maxTextLen       = 169 // payload budget: MAX_FRAME_SIZE 176 - 7 byte send header
	ackTimeout       = 15 * time.Second
	maxGroupChannels = 8

	selfInfoNameOff = 58
	channelInfoName = 34 // [0x12][idx][32 name][16 secret]
	// V2: [0x08][idx][path][txt_type][ts32][text] — text at 8, unlike V3
	channelMsgV2Text = 8
	channelMsgV3Text = 11
)

// meshMsg is a channel message off the wire, metadata included.
type meshMsg struct {
	Text   string
	Time   uint32 // mesh epoch seconds
	SNR    float32
	HasSNR bool
}

type companion struct {
	conn net.Conn
	rd   *bufio.Reader

	wmu  sync.Mutex  // frames to the radio
	reqM sync.Mutex  // one in-flight request at a time
	rmu  sync.Mutex  // guards resp
	resp chan []byte // ack of an in-flight request, nil when idle

	tsMu   sync.Mutex // guards lastTs
	lastTs uint32
}

func dialCompanion(addr string) (*companion, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	return &companion{conn: conn, rd: bufio.NewReader(conn)}, nil
}

func (c *companion) close() error {
	return c.conn.Close()
}

func (c *companion) send(payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	buf := make([]byte, 3+len(payload))
	buf[0] = '<'
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(payload)))
	copy(buf[3:], payload)
	_, err := c.conn.Write(buf)
	return err
}

// request sends one command frame and waits for a response frame whose opcode
// is in want (OK/ERROR/CLI-reply). Frames of other kinds are stale answers to
// earlier timed-out requests and are dropped rather than handed to the waiter.
func (c *companion) request(payload []byte, want ...byte) ([]byte, error) {
	c.reqM.Lock()
	defer c.reqM.Unlock()

	ack := make(chan []byte, 1)
	c.rmu.Lock()
	c.resp = ack
	c.rmu.Unlock()
	defer func() {
		c.rmu.Lock()
		c.resp = nil
		c.rmu.Unlock()
	}()

	if err := c.send(payload); err != nil {
		return nil, err
	}

	deadline := time.After(ackTimeout)
	for {
		select {
		case f := <-ack:
			for _, w := range want {
				if f[0] == w {
					return f, nil
				}
			}
		case <-deadline:
			return nil, errors.New("radio ack timeout")
		}
	}
}

// nextTs is a strictly increasing message timestamp: two sends in the same
// wall-clock second would otherwise share a ts and collide in `send.heard`.
func (c *companion) nextTs() uint32 {
	c.tsMu.Lock()
	defer c.tsMu.Unlock()
	ts := uint32(time.Now().Unix())
	if ts <= c.lastTs {
		ts = c.lastTs + 1
	}
	c.lastTs = ts
	return ts
}

// sendChannelText posts to the mesh channel and waits for the radio's OK/ERROR
// ack. Returns the message timestamp used, which `send.heard <ts>` keys on.
// An overlong text is cut on a rune boundary to fit the frame.
func (c *companion) sendChannelText(idx byte, text string) (uint32, error) {
	if len(text) > maxTextLen {
		text = truncate(text, maxTextLen)
	}
	ts := c.nextTs()
	payload := make([]byte, 7+len(text))
	payload[0] = cmdSendChannelTxtMsg
	payload[1] = txtTypePlain
	payload[2] = idx
	binary.LittleEndian.PutUint32(payload[3:], ts)
	copy(payload[7:], text)

	f, err := c.request(payload, pktOK, pktError)
	if err != nil {
		return 0, err
	}
	if f[0] != pktOK {
		code := byte(0)
		if len(f) > 1 {
			code = f[1]
		}
		return 0, fmt.Errorf("radio refused send: err 0x%02x", code)
	}
	return ts, nil
}

// runCLI runs a text CLI line on the radio and returns its reply text.
func (c *companion) runCLI(line string) (string, error) {
	f, err := c.request(append([]byte{cmdRunCLICommand}, line...), pktCliReply, pktError)
	if err != nil {
		return "", err
	}
	if f[0] != pktCliReply {
		code := byte(0)
		if len(f) > 1 {
			code = f[1]
		}
		return "", fmt.Errorf("radio refused CLI: err 0x%02x", code)
	}
	return trimNUL(f[1:]), nil
}

func (c *companion) deliverResp(f []byte) {
	c.rmu.Lock()
	ch := c.resp
	c.rmu.Unlock()
	if ch != nil {
		select {
		case ch <- f:
		default:
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func (c *companion) recv() ([]byte, error) {
	for {
		b, err := c.rd.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '>' {
			break
		}
	}
	var hdr [2]byte
	if _, err := io.ReadFull(c.rd, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint16(hdr[:])
	if n == 0 || n > maxFrameLen {
		return nil, fmt.Errorf("bad frame len %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.rd, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *companion) sendSync() error {
	return c.send([]byte{cmdSyncNextMessage})
}

// session: handshake, resolve channel index by name, then drain messages until
// the connection dies. Reconnect is the caller's loop. While the session lives,
// the bridge can push texts back into the mesh.
func session(addr, channelName string, b *bridge, deliver func(meshMsg) error) error {
	c, err := dialCompanion(addr)
	if err != nil {
		return err
	}
	defer c.close()

	// data[1] is the protocol version the app speaks; >= 3 gets V3 message frames
	if err := c.send([]byte{cmdAppStart, 3, 0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}
	name, err := c.waitSelfInfo()
	if err != nil {
		return err
	}
	log.Printf("radio: connected to %q", name)

	idx, err := c.resolveChannel(channelName)
	if err != nil {
		return err
	}
	log.Printf("radio: channel %q -> idx %d", channelName, idx)

	b.attach(c, idx)
	defer b.detach()

	return c.syncLoop(idx, deliver)
}

func (c *companion) waitSelfInfo() (string, error) {
	for {
		f, err := c.recv()
		if err != nil {
			return "", err
		}
		switch f[0] {
		case pktSelfInfo:
			name := ""
			if len(f) > selfInfoNameOff {
				name = trimNUL(f[selfInfoNameOff:])
			}
			return name, nil
		case pktError:
			code := byte(0)
			if len(f) > 1 {
				code = f[1]
			}
			return "", fmt.Errorf("APP_START failed: err 0x%02x", code)
		}
	}
}

func (c *companion) resolveChannel(name string) (byte, error) {
	found := map[string]byte{}
	for i := 0; i < maxGroupChannels; i++ {
		if err := c.send([]byte{cmdGetChannel, byte(i)}); err != nil {
			return 0, err
		}
		for {
			f, err := c.recv()
			if err != nil {
				return 0, err
			}
			if f[0] == pushMsgWaiting {
				continue
			}
			if f[0] == pktError {
				// our GET_CHANNEL pointed past the last slot on the radio
				return 0, fmt.Errorf("channel %q not found, radio has %v", name, found)
			}
			if f[0] == pktChannelInfo && len(f) >= channelInfoName {
				cname := trimNUL(f[2:channelInfoName])
				found[cname] = f[1]
				if cname == name {
					return f[1], nil
				}
				break
			}
		}
	}
	return 0, fmt.Errorf("channel %q not found, radio has %v", name, found)
}

// syncLoop: radio pushes MSG_WAITING, we pull with SYNC_NEXT_MESSAGE until
// NO_MORE_MSGS. A push seen mid-drain sets resync — the new message may have
// landed behind the iterator, so one more drain is kicked at the end.
func (c *companion) syncLoop(idx byte, deliver func(meshMsg) error) error {
	syncing := true
	resync := false
	if err := c.sendSync(); err != nil {
		return err
	}

	for {
		f, err := c.recv()
		if err != nil {
			return err
		}

		isMsg := false
		var msg meshMsg
		chIdx, plain := byte(0), false

		switch f[0] {
		case pushMsgWaiting:
			if syncing {
				resync = true
			} else {
				syncing = true
				if err := c.sendSync(); err != nil {
					return err
				}
			}
			continue
		case pktOK, pktError, pktCliReply:
			c.deliverResp(f)
			continue
		case pktNoMoreMsgs:
			syncing = false
			if resync {
				resync = false
				syncing = true
				if err := c.sendSync(); err != nil {
					return err
				}
			}
			continue
		case pktChannelMsgV3:
			if len(f) > channelMsgV3Text {
				chIdx, plain = f[4], f[6] == txtTypePlain
				msg = meshMsg{
					Text:   trimNUL(f[channelMsgV3Text:]),
					Time:   binary.LittleEndian.Uint32(f[7:11]),
					SNR:    float32(int8(f[1])) / 4,
					HasSNR: true,
				}
			}
			isMsg = true
		case pktChannelMsgV2:
			if len(f) > channelMsgV2Text {
				chIdx, plain = f[1], f[3] == txtTypePlain
				msg = meshMsg{
					Text: trimNUL(f[channelMsgV2Text:]),
					Time: binary.LittleEndian.Uint32(f[4:8]),
				}
			}
			isMsg = true
		case pktContactMsgV2, pktContactMsgV3, pktChannelData:
			isMsg = true
		}

		if isMsg {
			if plain && chIdx == idx && msg.Text != "" {
				if err := deliver(msg); err != nil {
					log.Printf("telegram: %v", err)
				}
			}
			if syncing {
				if err := c.sendSync(); err != nil {
					return err
				}
			}
		}
	}
}

func trimNUL(b []byte) string {
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return string(b)
}
