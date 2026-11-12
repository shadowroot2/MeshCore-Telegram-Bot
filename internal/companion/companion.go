package companion

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
	cmdSendTxtMsg        = 0x02
	cmdSendChannelTxtMsg = 0x03
	cmdGetContacts       = 0x04
	cmdSyncNextMessage   = 0x0A
	cmdGetChannel        = 0x1F
	cmdSetChannel        = 0x20
	cmdRunCLICommand     = 0x42

	pktOK           = 0x00
	pktError        = 0x01
	pktContactStart = 0x02
	pktContact      = 0x03
	pktContactEnd   = 0x04
	pktSelfInfo     = 0x05
	pktSent         = 0x06
	pktContactMsgV2 = 0x07
	pktChannelMsgV2 = 0x08
	pktNoMoreMsgs   = 0x0A
	pktContactMsgV3 = 0x10
	pktChannelMsgV3 = 0x11
	pktChannelInfo  = 0x12
	pktChannelData  = 0x1B
	pktCliReply     = 0x1D

	pushMsgWaiting = 0x83

	maxFrameLen      = 2048
	ackTimeout       = 15 * time.Second
	maxGroupChannels = 8

	selfInfoNameOff = 58
	channelInfoName = 34 // [0x12][idx][32 name][16 secret]
	// respQueue — глубина очереди ack'ов одного запроса; дамп контактов
	// приходит серией фреймов, которые помпа складывает сюда без блокировок
	respQueue = 256
	// msgsQueue — глубина очереди фреймов сообщений между помпой и syncLoop
	msgsQueue = 64
	// V2: [0x08][idx][path][txt_type][ts32][text] — text at 8, unlike V3
	channelMsgV2Text = 8
	channelMsgV3Text = 11
	// DM V2: [0x07][prefix6][path][txt_type][ts32][text], V3 adds [snr][0][0]
	contactMsgV2Text = 13
	contactMsgV3Text = 16
	contactPrefixOff = 1 // within [0x03] pubkey at 1
	contactPrefixLen = 6
	contactNameOff   = 100 // within [0x03]: 1+32+1+1+1+64
	contactFrameMin  = 148
)

// Text budgets for one send: MAX_FRAME_SIZE 176 minus the send header.
const (
	MaxChannelText = 169 // 7-byte channel header
	MaxDMText      = 163 // 13-byte DM header
)

// Коды ошибок из zephcore/app/CompanionProtocol.h.
const (
	errUnsupported = 0x01
	errNotFound    = 0x02
	errTableFull   = 0x03
	errBadState    = 0x04
	errIllegalArg  = 0x06
)

// RefusedError — радио явно отклонило запрос (error frame).
type RefusedError struct {
	Op   string
	Code byte
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("radio refused %s: err 0x%02x", e.Op, e.Code)
}

// Permanent — повтор того же запроса безнадёжен (нет контакта, чужой слот,
// неподдерживаемый тип). Временные отказы (таблица полна, mesh не готов)
// остаются на откуп повторам вызывающего.
func (e *RefusedError) Permanent() bool {
	switch e.Code {
	case errUnsupported, errNotFound, errIllegalArg:
		return true
	}
	return false
}

// Типы текста в DM/канальных фреймах (TxtDataHelpers.h).
const (
	TxtTypePlain      = 0 // обычное сообщение
	TxtTypeCLIData    = 1 // ответ удалённой командной строки
	TxtTypeCLICommand = 3 // команда удалённой командной строки (v1.18+)
)

// Kind says what a delivered Message is.
type Kind uint8

const (
	KindChannel Kind = iota
	KindDM
)

// Message is a channel or DM message off the wire, metadata included.
type Message struct {
	Kind       Kind
	ChannelIdx byte
	Prefix     [contactPrefixLen]byte // DM: sender pubkey prefix
	Text       string
	Time       uint32 // mesh epoch seconds
	SNR        float32
	HasSNR     bool
	TxtType    byte // Plain / CLIData / …
}

// SessionInfo is what the radio exposes after the handshake: every group
// channel by index, every contact name by pubkey prefix, and the index of
// the default channel (routing fallback).
type SessionInfo struct {
	NodeName   string // имя радио из SELF_INFO
	Addr       string // куда подключались (RADIO_ADDR)
	Channels   map[byte]string
	Contacts   map[[contactPrefixLen]byte]string
	DefaultIdx byte
}

// Hooks receives the live session so the caller can push texts into the mesh
// while Session drains messages out of it.
type Hooks interface {
	Attach(c *Client, info SessionInfo)
	Detach()
}

type Client struct {
	conn net.Conn
	rd   *bufio.Reader

	wmu  sync.Mutex  // frames to the radio
	reqM sync.Mutex  // one in-flight request at a time
	rmu  sync.Mutex  // guards resp
	resp chan []byte // ack of an in-flight request, nil when idle

}

func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rd: bufio.NewReader(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) send(payload []byte) error {
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
// is in want (OK/SENT/ERROR/CLI-reply). Frames of other kinds are stale
// answers to earlier timed-out requests or the bulk of a dump; the wrapper
// hands back only the matching frame.
func (c *Client) request(payload []byte, want ...byte) ([]byte, error) {
	frames, err := c.requestFrames(payload, want...)
	if err != nil {
		return nil, err
	}
	return frames[len(frames)-1], nil
}

// requestFrames sends one command and collects every frame until one of want
// arrives (that frame is last in the result). Intermediate frames are the
// bulk of a multi-frame dump (GET_CONTACTS) or stale answers to timed-out
// requests.
func (c *Client) requestFrames(payload []byte, want ...byte) ([][]byte, error) {
	c.reqM.Lock()
	defer c.reqM.Unlock()

	ack := make(chan []byte, respQueue)
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

	var frames [][]byte
	deadline := time.After(ackTimeout)
	for {
		select {
		case f := <-ack:
			frames = append(frames, f)
			for _, w := range want {
				if f[0] == w {
					return frames, nil
				}
			}
		case <-deadline:
			return frames, errors.New("radio ack timeout")
		}
	}
}

// SendChannelText posts to a mesh channel with the caller's message timestamp
// and waits for the radio's OK/ERROR ack. Retries must reuse the same ts:
// identical payload means an identical packet, which mesh dedup drops, so a
// resent message is not delivered twice. The ts keys `send.heard <ts>`.
func (c *Client) SendChannelText(idx byte, text string, ts uint32) error {
	text = Truncate(text, MaxChannelText)
	payload := make([]byte, 7+len(text))
	payload[0] = cmdSendChannelTxtMsg
	payload[1] = TxtTypePlain
	payload[2] = idx
	binary.LittleEndian.PutUint32(payload[3:], ts)
	copy(payload[7:], text)

	f, err := c.request(payload, pktOK, pktError)
	if err != nil {
		return err
	}
	if f[0] != pktOK {
		return radioErr("send", f)
	}
	return nil
}

// SendContactText sends a DM to a contact by pubkey prefix with the caller's
// timestamp and waits for the radio's PACKET_SENT ack. Retries must reuse ts.
func (c *Client) SendContactText(prefix [contactPrefixLen]byte, text string, ts uint32) error {
	return c.sendContact(prefix, text, ts, TxtTypePlain)
}

// SendContactCommand шлёт команду удалённой командной строки (TXT_TYPE_CLI_COMMAND)
// узлу контакта; ответ придёт отдельным DM с TXT_TYPE_CLI_DATA. Повторять
// нельзя: прошивка шлёт с ts из своего RTC, повтор выполнит команду дважды.
func (c *Client) SendContactCommand(prefix [contactPrefixLen]byte, text string, ts uint32) error {
	return c.sendContact(prefix, text, ts, TxtTypeCLICommand)
}

func (c *Client) sendContact(prefix [contactPrefixLen]byte, text string, ts uint32, txtType byte) error {
	text = Truncate(text, MaxDMText)
	payload := make([]byte, 13+len(text))
	payload[0] = cmdSendTxtMsg
	payload[1] = txtType
	payload[2] = 0 // first attempt
	binary.LittleEndian.PutUint32(payload[3:], ts)
	copy(payload[7:13], prefix[:])
	copy(payload[13:], text)

	f, err := c.request(payload, pktSent, pktError)
	if err != nil {
		return err
	}
	if f[0] != pktSent {
		return radioErr("DM send", f)
	}
	return nil
}

// QueryChannel returns the name and 16-byte secret of a channel slot.
func (c *Client) QueryChannel(idx byte) (string, [16]byte, error) {
	var secret [16]byte
	f, err := c.request([]byte{cmdGetChannel, idx}, pktChannelInfo, pktError)
	if err != nil {
		return "", secret, err
	}
	if f[0] != pktChannelInfo || len(f) < channelInfoName+16 {
		return "", secret, radioErr("GET_CHANNEL", f)
	}
	copy(secret[:], f[channelInfoName:channelInfoName+16])
	return trimNUL(f[2:channelInfoName]), secret, nil
}

// SetChannel writes a group channel slot: name (31 bytes max) and the shared
// 16-byte secret every member of the channel must have.
func (c *Client) SetChannel(idx byte, name string, secret [16]byte) error {
	payload := make([]byte, 50) // 1 + 1 + 32 + 16
	payload[0] = cmdSetChannel
	payload[1] = idx
	copy(payload[2:34], Truncate(name, 31))
	copy(payload[34:50], secret[:])
	f, err := c.request(payload, pktOK, pktError)
	if err != nil {
		return err
	}
	if f[0] != pktOK {
		return radioErr("SET_CHANNEL", f)
	}
	return nil
}

// RunCLI runs a text CLI line on the radio and returns its reply text.
func (c *Client) RunCLI(line string) (string, error) {
	f, err := c.request(append([]byte{cmdRunCLICommand}, line...), pktCliReply, pktError)
	if err != nil {
		return "", err
	}
	if f[0] != pktCliReply {
		return "", radioErr("CLI", f)
	}
	return trimNUL(f[1:]), nil
}

func radioErr(what string, f []byte) error {
	code := byte(0)
	if len(f) > 1 {
		code = f[1]
	}
	return &RefusedError{Op: what, Code: code}
}

func (c *Client) deliverResp(f []byte) {
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

// Truncate cuts s to at most n bytes on a rune boundary.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func (c *Client) recv() ([]byte, error) {
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

func (c *Client) sendSync() error {
	return c.send([]byte{cmdSyncNextMessage})
}

// Session: handshake, resolve channels and contacts, then drain messages
// (every channel and DM) until the connection dies. Reconnect is the caller's
// loop. While the session lives, the hooks can push texts back into the mesh.
//
// Помпа приёма стартует до хендшейка-резолва и Attach: запросы ждут ack,
// который маршрутизирует только она. Доставка сообщений идёт отдельным
// потребителем — медленный deliver (Telegram) не занимает приём ack'ов.
func Session(addr, defaultChannel string, hooks Hooks, deliver func(Message) error) error {
	c, err := Dial(addr)
	if err != nil {
		return err
	}
	defer c.Close()

	// data[1] is the protocol version the app speaks; >= 3 gets V3 message frames
	if err := c.send([]byte{cmdAppStart, 3, 0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}
	name, err := c.waitSelfInfo()
	if err != nil {
		return err
	}
	log.Printf("radio: connected to %q", name)

	frames := make(chan []byte, msgsQueue)
	done := make(chan error, 2)
	go func() {
		err := c.pump(frames)
		close(frames)
		done <- err
	}()
	go func() { done <- c.syncLoop(frames, deliver) }()

	info := SessionInfo{Contacts: map[[contactPrefixLen]byte]string{}}
	info.NodeName = name
	info.Addr = addr
	if err := c.resolveChannels(&info, defaultChannel); err != nil {
		return err
	}
	if contacts, err := c.Contacts(); err != nil {
		// contacts only name the DM topics; run without names rather than die
		log.Printf("radio: contacts sync failed: %v", err)
	} else {
		info.Contacts = contacts
	}
	log.Printf("radio: %d channels, %d contacts, default %q -> idx %d",
		len(info.Channels), len(info.Contacts), defaultChannel, info.DefaultIdx)

	hooks.Attach(c, info)
	defer hooks.Detach()

	return <-done
}

func (c *Client) waitSelfInfo() (string, error) {
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
			return "", fmt.Errorf("APP_START failed: %s", radioErr("APP_START", f))
		}
	}
}

// resolveChannels fills info.Channels for every non-empty slot and picks the
// default channel index by name. An error frame means the radio has no slot
// with that index — the scan is over.
func (c *Client) resolveChannels(info *SessionInfo, defaultName string) error {
	info.Channels = map[byte]string{}
	found := false
	for i := 0; i < maxGroupChannels; i++ {
		f, err := c.request([]byte{cmdGetChannel, byte(i)}, pktChannelInfo, pktError)
		if err != nil {
			return err
		}
		if f[0] == pktError {
			break
		}
		if len(f) < channelInfoName {
			return fmt.Errorf("GET_CHANNEL %d: short frame (%d bytes)", i, len(f))
		}
		cname := trimNUL(f[2:channelInfoName])
		if cname != "" {
			info.Channels[f[1]] = cname
			if cname == defaultName {
				info.DefaultIdx, found = f[1], true
			}
		}
	}
	if !found {
		return fmt.Errorf("channel %q not found, radio has %v", defaultName, info.Channels)
	}
	return nil
}

// Contacts dumps the radio's contact list: pubkey prefix -> name. Имена
// авто-добавленных контактов появляются только после их advert'ов, поэтому
// список можно перечитывать в любой момент — вызов безопасен и во время
// живой сессии: ответы маршрутизирует помпа, сокет он не делит ни с кем.
func (c *Client) Contacts() (map[[contactPrefixLen]byte]string, error) {
	frames, err := c.requestFrames([]byte{cmdGetContacts}, pktContactEnd, pktError)
	if err != nil {
		return nil, err
	}
	out := map[[contactPrefixLen]byte]string{}
	for _, f := range frames {
		if f[0] == pktError {
			return nil, radioErr("GET_CONTACTS", f)
		}
		if f[0] == pktContact && len(f) >= contactFrameMin {
			var prefix [contactPrefixLen]byte
			copy(prefix[:], f[contactPrefixOff:contactPrefixOff+contactPrefixLen])
			out[prefix] = trimNUL(f[contactNameOff : contactNameOff+32])
		}
	}
	return out, nil
}

// isRespFrame — фреймы-ответы на запросы клиент → радио. Их маршрутизирует
// помпа ожидающему запросчику; всё остальное — поток сообщений и sync-управление.
func isRespFrame(op byte) bool {
	switch op {
	case pktOK, pktError, pktSent, pktCliReply, pktChannelInfo,
		pktContactStart, pktContact, pktContactEnd:
		return true
	}
	return false
}

// pump — единственный читатель сокета: ответы на запросы уходят ожидающему
// запросчику (deliverResp), всё остальное — в канал фреймов syncLoop. Помпа
// не должна делать ничего, кроме чтения сокета и записи в канал, иначе
// живые запросы (отправка, CLI, GET_CONTACTS) начинают ловить ack timeout.
func (c *Client) pump(out chan<- []byte) error {
	for {
		f, err := c.recv()
		if err != nil {
			return err
		}
		if isRespFrame(f[0]) {
			c.deliverResp(f)
			continue
		}
		out <- f
	}
}

// syncLoop: radio pushes MSG_WAITING, we pull with SYNC_NEXT_MESSAGE until
// NO_MORE_MSGS. A push seen mid-drain sets resync — the new message may have
// landed behind the iterator, so one more drain is kicked at the end.
// Доставка сообщений идёт здесь; ack'и параллельно разносит помпа.
func (c *Client) syncLoop(frames <-chan []byte, deliver func(Message) error) error {
	syncing := true
	resync := false
	if err := c.sendSync(); err != nil {
		return err
	}

	for f := range frames {
		isMsg := false
		var msg Message

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
				msg = Message{
					Kind:       KindChannel,
					ChannelIdx: f[4],
					Text:       trimNUL(f[channelMsgV3Text:]),
					Time:       binary.LittleEndian.Uint32(f[7:11]),
					SNR:        float32(int8(f[1])) / 4,
					HasSNR:     true,
					TxtType:    f[6],
				}
			}
			isMsg = true
		case pktChannelMsgV2:
			if len(f) > channelMsgV2Text {
				msg = Message{
					Kind:       KindChannel,
					ChannelIdx: f[1],
					Text:       trimNUL(f[channelMsgV2Text:]),
					Time:       binary.LittleEndian.Uint32(f[4:8]),
					TxtType:    f[3],
				}
			}
			isMsg = true
		case pktContactMsgV3:
			if len(f) > contactMsgV3Text {
				var prefix [contactPrefixLen]byte
				copy(prefix[:], f[4:10])
				msg = Message{
					Kind:    KindDM,
					Prefix:  prefix,
					Text:    trimNUL(f[contactMsgV3Text:]),
					Time:    binary.LittleEndian.Uint32(f[12:16]),
					SNR:     float32(int8(f[1])) / 4,
					HasSNR:  true,
					TxtType: f[11],
				}
			}
			isMsg = true
		case pktContactMsgV2:
			if len(f) > contactMsgV2Text {
				var prefix [contactPrefixLen]byte
				copy(prefix[:], f[1:7])
				msg = Message{
					Kind:    KindDM,
					Prefix:  prefix,
					Text:    trimNUL(f[contactMsgV2Text:]),
					Time:    binary.LittleEndian.Uint32(f[9:13]),
					TxtType: f[8],
				}
			}
			isMsg = true
		case pktChannelData:
			isMsg = true
		}

		if isMsg {
			// plain — обычный текст, CLI_DATA — ответ удалённой командной
			// строки; остальное (CLI_COMMAND без допуска и т.п.) не доставляем
			if (msg.TxtType == TxtTypePlain || msg.TxtType == TxtTypeCLIData) && msg.Text != "" {
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
	return errors.New("radio connection closed")
}

func trimNUL(b []byte) string {
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return string(b)
}
