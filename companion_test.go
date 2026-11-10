package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

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

// fakeRadio speaks just enough companion protocol for session(): APP_START,
// GET_CHANNEL (idx 0 = Public, 1 = Other), a fixed message queue drained by
// SYNC_NEXT_MESSAGE, and OK-acked CMD_SEND_CHANNEL_TXT_MSG (text pushed to
// sent). After NO_MORE_MSGS it pushes MSG_WAITING once with one late message
// to exercise the push->pull path. With stale=true every request answer is
// preceded by a response frame of the wrong kind, like a late answer to a
// timed-out request would look.
func fakeRadio(t *testing.T, conn net.Conn, sent chan<- string, cliReply string, stale bool) {
	t.Helper()
	rd := bufio.NewReader(conn)
	queue := [][]byte{
		channelMsgV3Frame(0, txtTypePlain, 34, 1700000000, "hello"), // 34/4 = 8.5 dB
		channelMsgV3Frame(1, txtTypePlain, 34, 1700000000, "wrong channel"),
		channelMsgV3Frame(0, 9, 34, 1700000000, "wrong txt_type"),
		channelMsgV3Frame(0, txtTypePlain, 20, 1700000001, "world"),
		channelMsgV2Frame(0, txtTypePlain, 1700000002, "v2 hello"),
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

func waitOnline(t *testing.T, b *bridge) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !b.online() {
		if time.Now().After(deadline) {
			t.Fatal("radio never came online")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSessionDeliversPublicOnly(t *testing.T) {
	addr := startFakeRadio(t, nil, "> 2", false)

	var got []meshMsg
	done := make(chan struct{})
	deliver := func(m meshMsg) error {
		got = append(got, m)
		if m.Text == "late" {
			close(done)
		}
		return nil
	}

	go func() {
		_ = session(addr, "Public", &bridge{}, deliver)
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
	want := []string{"hello", "world", "v2 hello", "late"}
	if strings.Join(texts, ",") != strings.Join(want, ",") {
		t.Fatalf("delivered %v, want %v", texts, want)
	}

	if !got[0].HasSNR || got[0].SNR != 8.5 || got[0].Time != 1700000000 {
		t.Fatalf("hello metadata: %+v", got[0])
	}
	if got[2].HasSNR || got[2].Time != 1700000002 {
		t.Fatalf("v2 metadata: %+v", got[2])
	}
}

func TestBridgeSendsToMesh(t *testing.T) {
	sent := make(chan string, 4)
	addr := startFakeRadio(t, sent, "> 2", false)

	b := &bridge{}
	go func() {
		_ = session(addr, "Public", b, func(meshMsg) error { return nil })
	}()
	waitOnline(t, b)

	if _, err := b.sendText("ping из telegram"); err != nil {
		t.Fatalf("sendText: %v", err)
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

func TestTruncateKeepsRunes(t *testing.T) {
	s := "йцукенгшщз"
	out := truncate(s, 10) // cuts mid-'у' (2 bytes) if not rune-safe
	if !utf8.ValidString(out) || len(out) > 10 {
		t.Fatalf("truncate produced %q (%d bytes)", out, len(out))
	}
}

func TestFormatForTelegram(t *testing.T) {
	m := meshMsg{Text: "привет из меша", Time: 1700000000, SNR: 8.5, HasSNR: true}
	want := time.Unix(1700000000, 0).Format("15:04") + " · SNR 8.5 dB\nпривет из меша"
	if got := formatForTelegram(m); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	m2 := meshMsg{Text: "без SNR", Time: 1700000000}
	want2 := time.Unix(1700000000, 0).Format("15:04") + "\nбез SNR"
	if got := formatForTelegram(m2); got != want2 {
		t.Fatalf("got %q, want %q", got, want2)
	}

	m3 := meshMsg{Text: "без часов", SNR: 8.5, HasSNR: true}
	if got := formatForTelegram(m3); got != "SNR 8.5 dB\nбез часов" {
		t.Fatalf("got %q", got)
	}

	m4 := meshMsg{Text: "голый текст"}
	if got := formatForTelegram(m4); got != "голый текст" {
		t.Fatalf("got %q", got)
	}
}

func TestMeshText(t *testing.T) {
	m := &tgMessage{Text: "  привет  "}
	m.From.FirstName = "Иван"
	if got := meshText(m); got != "Иван: привет" {
		t.Fatalf("got %q", got)
	}

	m2 := &tgMessage{Text: "   \n\t "}
	if got := meshText(m2); got != "" {
		t.Fatalf("whitespace-only must be skipped, got %q", got)
	}

	m3 := &tgMessage{Text: "текст"}
	m3.From.FirstName = strings.Repeat("Д", 40)
	got := meshText(m3)
	if !strings.HasSuffix(got, ": текст") {
		t.Fatalf("body must survive a long name, got %q", got)
	}
	name := strings.TrimSuffix(got, ": текст")
	if len(name) > maxNameLen {
		t.Fatalf("name not capped: %d bytes", len(name))
	}
}

func TestSenderName(t *testing.T) {
	m := &tgMessage{}
	m.From.FirstName = "Иван"
	m.From.LastName = "Петров"
	m.From.Username = "ivan"
	m.From.ID = 42
	if got := senderName(m); got != "Иван Петров" {
		t.Fatalf("got %q", got)
	}

	m2 := &tgMessage{}
	m2.From.Username = "ivan"
	if got := senderName(m2); got != "@ivan" {
		t.Fatalf("got %q", got)
	}

	m3 := &tgMessage{}
	m3.From.ID = 42
	if got := senderName(m3); got != "id 42" {
		t.Fatalf("got %q", got)
	}
}

func TestHeardCountViaCLI(t *testing.T) {
	addr := startFakeRadio(t, nil, "> 2", false)

	b := &bridge{}
	go func() {
		_ = session(addr, "Public", b, func(meshMsg) error { return nil })
	}()
	waitOnline(t, b)

	n, err := b.heardCount(1700000000)
	if err != nil {
		t.Fatalf("heardCount: %v", err)
	}
	if n != 2 {
		t.Fatalf("heardCount = %d, want 2", n)
	}
}

func TestHeardCountGarbageReply(t *testing.T) {
	addr := startFakeRadio(t, nil, "Unknown command", false)

	b := &bridge{}
	go func() {
		_ = session(addr, "Public", b, func(meshMsg) error { return nil })
	}()
	waitOnline(t, b)

	n, err := b.heardCount(1700000000)
	if err != nil {
		t.Fatalf("heardCount: %v", err)
	}
	if n != -1 {
		t.Fatalf("heardCount = %d, want -1 for unparseable reply", n)
	}
}

func TestRequestFiltersStaleFrames(t *testing.T) {
	addr := startFakeRadio(t, nil, "> 2", true)

	b := &bridge{}
	go func() {
		_ = session(addr, "Public", b, func(meshMsg) error { return nil })
	}()
	waitOnline(t, b)

	if _, err := b.sendText("stale ok"); err != nil {
		t.Fatalf("sendText must ignore the stale CLI reply: %v", err)
	}
	n, err := b.heardCount(1700000000)
	if err != nil {
		t.Fatalf("heardCount must ignore the stale OK: %v", err)
	}
	if n != 2 {
		t.Fatalf("heardCount = %d, want 2", n)
	}
}

func TestSendTimestampsAreUnique(t *testing.T) {
	addr := startFakeRadio(t, nil, "> 2", false)

	b := &bridge{}
	go func() {
		_ = session(addr, "Public", b, func(meshMsg) error { return nil })
	}()
	waitOnline(t, b)

	ts1, err := b.sendText("one")
	if err != nil {
		t.Fatal(err)
	}
	ts2, err := b.sendText("two")
	if err != nil {
		t.Fatal(err)
	}
	if ts2 <= ts1 {
		t.Fatalf("timestamps must be strictly increasing, got %d then %d", ts1, ts2)
	}
}

func TestFormatHeard(t *testing.T) {
	cases := map[int]string{
		-1: "нет данных о доставке",
		0:  "никто не услышал",
		1:  "1 репиттер услышал",
		2:  "2 репиттера услышали",
		5:  "5 репиттеров услышали",
		12: "12 репиттеров услышали",
		21: "21 репиттер услышал",
	}
	for n, want := range cases {
		if got := formatHeard(n); got != want {
			t.Fatalf("formatHeard(%d) = %q, want %q", n, got, want)
		}
	}
}
