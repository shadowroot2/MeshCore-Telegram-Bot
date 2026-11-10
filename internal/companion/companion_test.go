package companion

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// hooksPair is a Hooks stand-in that exposes the attached session to tests.
type hooksPair struct {
	mu   sync.Mutex
	c    *Client
	info SessionInfo
}

func (h *hooksPair) Attach(c *Client, info SessionInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.c, h.info = c, info
}

func (h *hooksPair) Detach() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.c = nil
}

func (h *hooksPair) client() (*Client, SessionInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.c, h.info
}

func writeFrame(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	buf := make([]byte, 3+len(payload))
	buf[0] = '>'
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(payload)))
	copy(buf[3:], payload)
	if _, err := conn.Write(buf); err != nil {
		t.Errorf("write frame: %v", err)
	}
}

func readFrame(t *testing.T, rd *bufio.Reader) []byte {
	t.Helper()
	for {
		b, err := rd.ReadByte()
		if err != nil {
			t.Errorf("read sync: %v", err)
			return nil
		}
		if b == '<' {
			break
		}
	}
	var hdr [2]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		t.Errorf("read len: %v", err)
		return nil
	}
	payload := make([]byte, binary.LittleEndian.Uint16(hdr[:]))
	if _, err := io.ReadFull(rd, payload); err != nil {
		t.Errorf("read payload: %v", err)
		return nil
	}
	return payload
}

func selfInfoFrame(name string) []byte {
	f := make([]byte, selfInfoNameOff+len(name))
	f[0] = pktSelfInfo
	copy(f[selfInfoNameOff:], name)
	return f
}

func channelInfoFrame(idx byte, name string) []byte {
	f := make([]byte, channelInfoName+16)
	f[0] = pktChannelInfo
	f[1] = idx
	copy(f[2:34], name)
	return f
}

func channelMsgV3Frame(idx, txtType byte, snr int8, ts uint32, text string) []byte {
	f := make([]byte, channelMsgV3Text+len(text))
	f[0] = pktChannelMsgV3
	f[1] = byte(snr)
	f[4] = idx
	f[6] = txtType
	binary.LittleEndian.PutUint32(f[7:11], ts)
	copy(f[channelMsgV3Text:], text)
	return f
}

func channelMsgV2Frame(idx, txtType byte, ts uint32, text string) []byte {
	f := make([]byte, channelMsgV2Text+len(text))
	f[0] = pktChannelMsgV2
	f[1] = idx
	f[2] = 0xFF
	f[3] = txtType
	binary.LittleEndian.PutUint32(f[4:8], ts)
	copy(f[channelMsgV2Text:], text)
	return f
}

func contactMsgV3Frame(prefix [6]byte, txtType byte, snr int8, ts uint32, text string) []byte {
	f := make([]byte, contactMsgV3Text+len(text))
	f[0] = pktContactMsgV3
	f[1] = byte(snr)
	copy(f[4:10], prefix[:])
	f[10] = 0xFF
	f[11] = txtType
	binary.LittleEndian.PutUint32(f[12:16], ts)
	copy(f[contactMsgV3Text:], text)
	return f
}

func contactMsgV2Frame(prefix [6]byte, txtType byte, ts uint32, text string) []byte {
	f := make([]byte, contactMsgV2Text+len(text))
	f[0] = pktContactMsgV2
	copy(f[1:7], prefix[:])
	f[7] = 0xFF
	f[8] = txtType
	binary.LittleEndian.PutUint32(f[9:13], ts)
	copy(f[contactMsgV2Text:], text)
	return f
}

func contactFrame(prefix [6]byte, name string) []byte {
	f := make([]byte, contactFrameMin)
	f[0] = pktContact
	copy(f[contactPrefixOff:contactPrefixOff+contactPrefixLen], prefix[:])
	copy(f[contactNameOff:contactNameOff+32], name)
	return f
}

var testPrefix = [6]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01}

// fakeRadio speaks just enough companion protocol for Session(): APP_START,
// GET_CHANNEL (0 = Public, 1 = Other, rest empty), GET_CONTACTS, OK-acked
// channel sends, SENT-acked DM sends, and a fixed message queue drained by
// SYNC_NEXT_MESSAGE. After NO_MORE_MSGS it pushes MSG_WAITING once with one
// late message. With stale=true every request answer is preceded by a response
// frame of the wrong kind, like a late answer to a timed-out request.
func fakeRadio(t *testing.T, conn net.Conn, sent chan<- string, cliReply string, stale bool) {
	t.Helper()
	rd := bufio.NewReader(conn)
	queue := [][]byte{
		channelMsgV3Frame(0, txtTypePlain, 34, 1700000000, "hello"), // 34/4 = 8.5 dB
		channelMsgV3Frame(1, txtTypePlain, 34, 1700000000, "wrong channel"),
		channelMsgV3Frame(0, 9, 34, 1700000000, "wrong txt_type"),
		channelMsgV3Frame(0, txtTypePlain, 20, 1700000001, "world"),
		channelMsgV2Frame(0, txtTypePlain, 1700000002, "v2 hello"),
		contactMsgV3Frame(testPrefix, txtTypePlain, 30, 1700000004, "dm hello"),
		contactMsgV2Frame(testPrefix, txtTypePlain, 1700000005, "dm v2 hello"),
	}
	lateSent := false

	for {
		f := readFrame(t, rd)
		if f == nil {
			return
		}
		switch f[0] {
		case cmdAppStart:
			writeFrame(t, conn, selfInfoFrame("test-radio"))
		case cmdGetChannel:
			switch f[1] {
			case 0:
				writeFrame(t, conn, channelInfoFrame(0, "Public"))
			case 1:
				writeFrame(t, conn, channelInfoFrame(1, "Other"))
			default:
				writeFrame(t, conn, []byte{pktError, 0x06})
			}
		case cmdGetContacts:
			writeFrame(t, conn, []byte{pktContactStart, 1, 0, 0, 0})
			writeFrame(t, conn, contactFrame(testPrefix, "Валера"))
			writeFrame(t, conn, []byte{pktContactEnd, 0, 0, 0, 0})
		case cmdSendTxtMsg:
			text := ""
			if len(f) > 13 {
				text = string(f[13:])
			}
			if sent != nil {
				sent <- "dm:" + text
			}
			if stale {
				writeFrame(t, conn, append([]byte{pktCliReply}, "stale"...))
			}
			writeFrame(t, conn, []byte{pktSent, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		case cmdSendChannelTxtMsg:
			text := ""
			if len(f) > 7 {
				text = string(f[7:])
			}
			if sent != nil {
				sent <- text
			}
			if stale {
				writeFrame(t, conn, append([]byte{pktCliReply}, "stale"...))
			}
			writeFrame(t, conn, []byte{pktOK})
		case cmdRunCLICommand:
			if stale {
				writeFrame(t, conn, []byte{pktOK})
			}
			writeFrame(t, conn, append([]byte{pktCliReply}, cliReply...))
		case cmdSyncNextMessage:
			if len(queue) > 0 {
				writeFrame(t, conn, queue[0])
				queue = queue[1:]
				continue
			}
			writeFrame(t, conn, []byte{pktNoMoreMsgs})
			if !lateSent {
				lateSent = true
				writeFrame(t, conn, []byte{pushMsgWaiting})
				queue = append(queue, channelMsgV3Frame(0, txtTypePlain, 34, 1700000003, "late"))
			}
		}
	}
}

func startFakeRadio(t *testing.T, sent chan<- string, cliReply string, stale bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		fakeRadio(t, conn, sent, cliReply, stale)
	}()
	return ln.Addr().String()
}

func startSession(t *testing.T, addr string) *hooksPair {
	t.Helper()
	h := &hooksPair{}
	go func() {
		_ = Session(addr, "Public", h, func(Message) error { return nil })
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if c, _ := h.client(); c != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("radio never came online")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return h
}

func TestSessionResolvesChannelsAndContacts(t *testing.T) {
	h := startSession(t, startFakeRadio(t, nil, "> 2", false))
	_, info := h.client()

	if info.DefaultIdx != 0 {
		t.Fatalf("default idx = %d", info.DefaultIdx)
	}
	if info.Channels[0] != "Public" || info.Channels[1] != "Other" || len(info.Channels) != 2 {
		t.Fatalf("channels: %v", info.Channels)
	}
	if info.Contacts[testPrefix] != "Валера" {
		t.Fatalf("contacts: %v", info.Contacts)
	}
}

func TestSessionDeliversChannelsAndDMs(t *testing.T) {
	addr := startFakeRadio(t, nil, "> 2", false)

	var got []Message
	done := make(chan struct{})
	deliver := func(m Message) error {
		got = append(got, m)
		if m.Text == "late" {
			close(done)
		}
		return nil
	}

	go func() {
		_ = Session(addr, "Public", &hooksPair{}, deliver)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout, delivered so far: %+v", got)
	}

	var texts []string
	for _, m := range got {
		texts = append(texts, m.Text)
	}
	// все каналы и DM доставляются: фильтрация — забота роутинга в bridge
	want := []string{"hello", "wrong channel", "world", "v2 hello", "dm hello", "dm v2 hello", "late"}
	if strings.Join(texts, ",") != strings.Join(want, ",") {
		t.Fatalf("delivered %v, want %v", texts, want)
	}

	if got[0].Kind != KindChannel || got[0].ChannelIdx != 0 || !got[0].HasSNR || got[0].SNR != 8.5 {
		t.Fatalf("hello metadata: %+v", got[0])
	}
	if got[1].Kind != KindChannel || got[1].ChannelIdx != 1 {
		t.Fatalf("other-channel metadata: %+v", got[1])
	}
	if got[3].HasSNR || got[3].Time != 1700000002 {
		t.Fatalf("v2 metadata: %+v", got[3])
	}
	if got[4].Kind != KindDM || got[4].Prefix != testPrefix || got[4].Text != "dm hello" {
		t.Fatalf("dm metadata: %+v", got[4])
	}
	if got[5].Kind != KindDM || got[5].Time != 1700000005 {
		t.Fatalf("dm v2 metadata: %+v", got[5])
	}
}

func TestSendChannelText(t *testing.T) {
	sent := make(chan string, 4)
	h := startSession(t, startFakeRadio(t, sent, "> 2", false))
	c, info := h.client()

	if _, err := c.SendChannelText(info.DefaultIdx, "ping из telegram"); err != nil {
		t.Fatalf("SendChannelText: %v", err)
	}

	select {
	case s := <-sent:
		if s != "ping из telegram" {
			t.Fatalf("mesh got %q", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fake radio never saw the text")
	}
}

func TestSendContactText(t *testing.T) {
	sent := make(chan string, 4)
	h := startSession(t, startFakeRadio(t, sent, "> 2", false))
	c, _ := h.client()

	if _, err := c.SendContactText(testPrefix, "личка"); err != nil {
		t.Fatalf("SendContactText: %v", err)
	}

	select {
	case s := <-sent:
		if s != "dm:личка" {
			t.Fatalf("mesh got %q", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fake radio never saw the DM")
	}
}

func TestRunCLI(t *testing.T) {
	h := startSession(t, startFakeRadio(t, nil, "> 2", false))
	c, _ := h.client()

	reply, err := c.RunCLI("send.heard 1700000000")
	if err != nil {
		t.Fatalf("RunCLI: %v", err)
	}
	if reply != "> 2" {
		t.Fatalf("reply = %q", reply)
	}
}

func TestQueryChannel(t *testing.T) {
	h := startSession(t, startFakeRadio(t, nil, "> 2", false))
	c, _ := h.client()

	name, _, err := c.QueryChannel(1)
	if err != nil || name != "Other" {
		t.Fatalf("QueryChannel(1) = %q, %v", name, err)
	}
	if _, _, err := c.QueryChannel(5); err == nil {
		t.Fatal("empty slot must fail")
	}
}

func TestRequestFiltersStaleFrames(t *testing.T) {
	h := startSession(t, startFakeRadio(t, nil, "> 2", true))
	c, info := h.client()

	if _, err := c.SendChannelText(info.DefaultIdx, "stale ok"); err != nil {
		t.Fatalf("send must ignore the stale CLI reply: %v", err)
	}
	if _, err := c.SendContactText(testPrefix, "stale dm"); err != nil {
		t.Fatalf("DM send must ignore the stale CLI reply: %v", err)
	}
	reply, err := c.RunCLI("send.heard 1700000000")
	if err != nil {
		t.Fatalf("CLI must ignore the stale OK: %v", err)
	}
	if reply != "> 2" {
		t.Fatalf("reply = %q", reply)
	}
}

func TestSendTimestampsAreUnique(t *testing.T) {
	h := startSession(t, startFakeRadio(t, nil, "> 2", false))
	c, info := h.client()

	ts1, err := c.SendChannelText(info.DefaultIdx, "one")
	if err != nil {
		t.Fatal(err)
	}
	ts2, err := c.SendContactText(testPrefix, "two")
	if err != nil {
		t.Fatal(err)
	}
	if ts2 <= ts1 {
		t.Fatalf("timestamps must be strictly increasing, got %d then %d", ts1, ts2)
	}
}

func TestTruncateKeepsRunes(t *testing.T) {
	s := "йцукенгшщз"
	out := Truncate(s, 10) // cuts mid-'у' (2 bytes) if not rune-safe
	if !utf8.ValidString(out) || len(out) > 10 {
		t.Fatalf("truncate produced %q (%d bytes)", out, len(out))
	}
}
