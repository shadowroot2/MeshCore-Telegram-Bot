package bridge

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/companion"
	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/telegram"
)

// heardCheckDelay matches the firmware's CHANNEL_FEEDBACK_WINDOW_MS: that
// long after the send the retransmit dupes are worth extracting. A var so
// tests can shorten it.
var heardCheckDelay = 5 * time.Second

const maxNameLen = 32 // sender name budget before it eats the mesh text

// radio is the mesh side the bridge needs; *companion.Client implements it.
type radio interface {
	SendChannelText(idx byte, text string) (uint32, error)
	SendContactText(prefix [6]byte, text string) (uint32, error)
	RunCLI(line string) (string, error)
}

// Telegram is the Bot API surface the bridge needs; *telegram.Client
// implements it.
type Telegram interface {
	ChatID() int64
	GetUpdates(offset int64) ([]telegram.Update, error)
	SendMessage(text string, threadID int64) error
	ReplyToMessage(threadID, replyToMsgID int64, text string) error
	CreateForumTopic(name string) (int64, error)
}

// target is one mesh endpoint and its Telegram topic.
type target struct {
	kind    companion.Kind
	channel byte
	prefix  [6]byte
	key     string // registry key: "ch:<idx>" or "dm:<prefix hex>"
	name    string // topic title
}

// topics maps mesh endpoints to Telegram forum topic ids and persists the
// mapping so restarts reuse topics instead of creating duplicates.
type topics struct {
	mu    sync.Mutex
	path  string
	byKey map[string]int64
	byID  map[int64]string
}

func loadTopics(path string) *topics {
	t := &topics{path: path, byKey: map[string]int64{}, byID: map[int64]string{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return t
	}
	var raw map[string]int64
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Printf("topics: %s: %v", path, err)
		return t
	}
	for k, id := range raw {
		t.byKey[k], t.byID[id] = id, k
	}
	return t
}

func (t *topics) thread(key string) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id, ok := t.byKey[key]
	return id, ok
}

func (t *topics) key(thread int64) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.byID[thread]
	return k, ok
}

func (t *topics) bind(key string, thread int64) {
	t.mu.Lock()
	t.byKey[key], t.byID[thread] = thread, key
	raw := make(map[string]int64, len(t.byKey))
	for k, id := range t.byKey {
		raw[k] = id
	}
	t.mu.Unlock()

	data, err := json.Marshal(raw)
	if err == nil {
		err = os.WriteFile(t.path, data, 0o600)
	}
	if err != nil {
		log.Printf("topics: save %s: %v", t.path, err)
	}
}

// ensure returns the topic for key, creating it once; 0 means General.
func (t *topics) ensure(key, name string, tg Telegram) int64 {
	if id, ok := t.thread(key); ok {
		return id
	}
	id, err := tg.CreateForumTopic(name)
	if err != nil {
		log.Printf("topics: create %q: %v (posting to General)", name, err)
		return 0
	}
	t.bind(key, id)
	return id
}

// Bridge wires the mesh session to the Telegram forum group: one вкладка per
// channel and per DM contact.
type Bridge struct {
	mu     sync.Mutex
	r      radio
	info   companion.SessionInfo
	tg     Telegram
	topics *topics
}

func New(tg Telegram, topicsFile string) *Bridge {
	return &Bridge{tg: tg, topics: loadTopics(topicsFile)}
}

// Attach implements companion.Hooks: remembers the session and makes sure
// every channel has its topic.
func (b *Bridge) Attach(c *companion.Client, info companion.SessionInfo) {
	b.mu.Lock()
	b.r, b.info = c, info
	b.mu.Unlock()

	for idx, name := range info.Channels {
		b.topics.ensure(channelKey(idx), name, b.tg)
	}
}

// Detach implements companion.Hooks.
func (b *Bridge) Detach() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.r = nil
}

func (b *Bridge) Online() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.r != nil
}

// Deliver routes a mesh message into its Telegram topic.
func (b *Bridge) Deliver(m companion.Message) error {
	t := b.targetFor(m)
	thread := b.topics.ensure(t.key, t.name, b.tg)
	return b.tg.SendMessage(formatForTelegram(m), thread)
}

func (b *Bridge) targetFor(m companion.Message) target {
	b.mu.Lock()
	defer b.mu.Unlock()
	if m.Kind == companion.KindDM {
		name := b.info.Contacts[m.Prefix]
		if name == "" {
			name = "dm-" + fmt.Sprintf("%x", m.Prefix)[:4]
		}
		return target{
			kind:   companion.KindDM,
			prefix: m.Prefix,
			key:    dmKey(m.Prefix),
			name:   name,
		}
	}
	name := b.info.Channels[m.ChannelIdx]
	if name == "" {
		name = fmt.Sprintf("channel-%d", m.ChannelIdx)
	}
	return target{
		kind:    companion.KindChannel,
		channel: m.ChannelIdx,
		key:     channelKey(m.ChannelIdx),
		name:    name,
	}
}

// targetFromThread maps a Telegram topic back to a mesh endpoint. Messages
// outside any вкладка (General) go to the default channel; topics the bridge
// did not create are ignored.
func (b *Bridge) targetFromThread(m *telegram.Message) (target, bool) {
	b.mu.Lock()
	info := b.info
	b.mu.Unlock()

	if !m.IsTopicMessage || m.MessageThreadID <= 0 {
		return target{
			kind:    companion.KindChannel,
			channel: info.DefaultIdx,
			key:     channelKey(info.DefaultIdx),
		}, true
	}
	key, ok := b.topics.key(m.MessageThreadID)
	if !ok {
		return target{}, false
	}
	if kind, arg, ok := strings.Cut(key, ":"); ok {
		switch kind {
		case "ch":
			idx, err := strconv.ParseUint(arg, 10, 8)
			if err == nil {
				return target{kind: companion.KindChannel, channel: byte(idx), key: key}, true
			}
		case "dm":
			var prefix [6]byte
			raw, err := hex.DecodeString(arg)
			if err == nil && len(raw) == len(prefix) {
				copy(prefix[:], raw)
				return target{kind: companion.KindDM, prefix: prefix, key: key}, true
			}
		}
	}
	return target{}, false
}

func (b *Bridge) sendTo(t target, text string) (uint32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.r == nil {
		return 0, errors.New("radio offline")
	}
	if t.kind == companion.KindDM {
		return b.r.SendContactText(t.prefix, text)
	}
	return b.r.SendChannelText(t.channel, text)
}

// HeardCount asks the radio how many neighbours repeated the flood for the
// send with that message timestamp (CLI `send.heard <ts>`). An unparseable
// reply (e.g. a firmware without `send.heard`) is reported as -1.
func (b *Bridge) HeardCount(ts uint32) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.r == nil {
		return 0, errors.New("radio offline")
	}
	reply, err := b.r.RunCLI(fmt.Sprintf("send.heard %d", ts))
	if err != nil {
		return 0, err
	}
	reply = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(reply), ">"))
	n, err := strconv.Atoi(reply)
	if err != nil {
		log.Printf("heard check: unparseable CLI reply %q", reply)
		return -1, nil
	}
	return n, nil
}

// PollTelegram: long-poll the group and push each text into the mesh. The
// Telegram offset advances only after the radio acked, so a down radio
// redelivers instead of dropping messages.
func PollTelegram(b *Bridge) {
	var offset int64
	for {
		updates, err := b.tg.GetUpdates(offset)
		if err != nil {
			log.Printf("telegram: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		next := processUpdates(b, offset, updates)
		if next == offset {
			// batch stuck on an undelivered message; back off before redelivery
			time.Sleep(3 * time.Second)
		}
		offset = next
	}
}

// processUpdates handles one getUpdates batch and returns the new offset.
// The offset advances past every handled message; one the radio refused keeps
// its offset, so Telegram redelivers it (at-least-once). A return equal to the
// input offset means the batch is stuck and the caller should back off.
func processUpdates(b *Bridge, offset int64, updates []telegram.Update) int64 {
	for _, u := range updates {
		m := u.Message
		if m == nil || m.Chat.ID != b.tg.ChatID() || m.From.IsBot {
			offset = u.UpdateID + 1
			continue
		}
		text := meshText(m)
		if text == "" {
			offset = u.UpdateID + 1
			continue
		}
		tgt, ok := b.targetFromThread(m)
		if !ok {
			log.Printf("mesh: unknown topic (thread %d), skipping", m.MessageThreadID)
			offset = u.UpdateID + 1
			continue
		}
		ts, err := b.sendTo(tgt, text)
		if err != nil {
			log.Printf("mesh: %v, will retry %q", err, text)
			return offset
		}
		offset = u.UpdateID + 1
		if tgt.kind == companion.KindChannel {
			// retransmit feedback only exists for channel floods
			go checkHeard(b, m, ts)
		}
	}
	return offset
}

// checkHeard replies to the user's message with how many repeaters repeated
// the flood. Stays silent when the radio is unreachable at check time.
func checkHeard(b *Bridge, m *telegram.Message, ts uint32) {
	time.Sleep(heardCheckDelay)
	n, err := b.HeardCount(ts)
	if err != nil {
		log.Printf("heard check: %v", err)
		return
	}
	if err := b.tg.ReplyToMessage(m.MessageThreadID, m.MessageID, formatHeard(n)); err != nil {
		log.Printf("telegram: %v", err)
	}
}

func channelKey(idx byte) string { return fmt.Sprintf("ch:%d", idx) }

func dmKey(prefix [6]byte) string { return fmt.Sprintf("dm:%x", prefix) }

// formatForTelegram renders the mesh metadata header: "18:37 · SNR 8.5 dB"
// on its own line above the text. A zero mesh timestamp (no clock on the
// sending node) is left out rather than shown as epoch.
func formatForTelegram(m companion.Message) string {
	head := ""
	if m.Time > 0 {
		head = time.Unix(int64(m.Time), 0).Format("15:04")
	}
	if m.HasSNR {
		if head != "" {
			head += " · "
		}
		head += fmt.Sprintf("SNR %.1f dB", m.SNR)
	}
	if head == "" {
		return m.Text
	}
	return head + "\n" + m.Text
}

// formatHeard renders the repeater-count reply.
func formatHeard(n int) string {
	switch {
	case n < 0:
		return "нет данных о доставке"
	case n == 0:
		return "никто не услышал"
	default:
		return fmt.Sprintf("%d %s %s", n,
			plural(n, "репиттер", "репиттера", "репиттеров"),
			plural(n, "услышал", "услышали", "услышали"))
	}
}

// meshText is what goes into the mesh for a Telegram message: "<name>: <text>".
// Whitespace-only messages yield "" and are skipped; a very long display name
// is cut so the message body still fits the frame.
func meshText(m *telegram.Message) string {
	body := strings.TrimSpace(m.Text)
	if body == "" {
		return ""
	}
	name := companion.Truncate(senderName(m), maxNameLen)
	return name + ": " + body
}

func senderName(m *telegram.Message) string {
	name := strings.TrimSpace(m.From.FirstName + " " + m.From.LastName)
	if name == "" && m.From.Username != "" {
		name = "@" + m.From.Username
	}
	if name == "" {
		name = fmt.Sprintf("id %d", m.From.ID)
	}
	return name
}

func plural(n int, one, few, many string) string {
	switch {
	case n%100 >= 11 && n%100 <= 14:
		return many
	case n%10 == 1:
		return one
	case n%10 >= 2 && n%10 <= 4:
		return few
	default:
		return many
	}
}
