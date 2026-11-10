package bridge

import (
	"crypto/rand"
	"crypto/sha256"
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
	"unicode/utf8"

	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/companion"
	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/telegram"
)

// heardCheckDelay matches the firmware's CHANNEL_FEEDBACK_WINDOW_MS: that
// long after the send the retransmit dupes are worth extracting. A var so
// tests can shorten it.
var heardCheckDelay = 5 * time.Second

const (
	maxNameLen      = 32 // sender name budget before it eats the mesh text
	maxChannelSlots = 8  // group channel slots on the radio
	// вкладки пробуются по thread_id (Bot API не даёт список тем): id идут
	// вслед за id сообщений группы, поэтому ограниченный обход с обрывом
	// после серии промахов покрывает обычные группы
	topicProbeMax    = 300
	topicProbeMisses = 30
)

// radio is the mesh side the bridge needs; *companion.Client implements it.
type radio interface {
	SendChannelText(idx byte, text string) (uint32, error)
	SendContactText(prefix [6]byte, text string) (uint32, error)
	QueryChannel(idx byte) (string, [16]byte, error)
	SetChannel(idx byte, name string, secret [16]byte) error
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
	GetForumTopic(threadID int64) (string, error)
	ListForumTopics(maxThreadID, maxMisses int64) map[string]int64
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
		log.Printf("topics: %s: %v (start empty — все темы будут созданы заново)", path, err)
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
	log.Printf("topics: %s: %d mappings loaded", path, len(t.byKey))
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
		log.Printf("topics: save %s: %v (маппинг не переживёт перезапуск!)", t.path, err)
	}
}

// ensure returns the topic for key, creating it once; 0 means General.
// Creation is retried: Telegram rate-limits topic creation, and a failed
// attempt would otherwise park the whole topic in General forever.
func (t *topics) ensure(key, name string, tg Telegram) int64 {
	if id, ok := t.thread(key); ok {
		return id
	}
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		var id int64
		id, err = tg.CreateForumTopic(name)
		if err == nil {
			t.bind(key, id)
			return id
		}
		log.Printf("topics: create %q: %v (retrying)", name, err)
		time.Sleep(time.Second)
	}
	log.Printf("topics: create %q: %v (posting to General)", name, err)
	return 0
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

// Attach implements companion.Hooks: remembers the session and syncs the
// topics with the group.
func (b *Bridge) Attach(c *companion.Client, info companion.SessionInfo) {
	b.mu.Lock()
	b.r, b.info = c, info
	b.mu.Unlock()

	b.syncTopics()
}

// syncTopics: существующие вкладки привязываются по имени, недостающие —
// создаются; ключи #-каналов приводятся к стандартному Public.
func (b *Bridge) syncTopics() {
	b.bindExistingTopics()

	b.mu.Lock()
	channels := make(map[byte]string, len(b.info.Channels))
	for idx, name := range b.info.Channels {
		channels[idx] = name
	}
	b.mu.Unlock()

	for idx, name := range channels {
		if _, secret, err := b.r.QueryChannel(idx); err == nil {
			b.normalizePublicChannel(idx, name, secret)
		}
		b.topics.ensure(channelKey(idx), name, b.tg)
		time.Sleep(250 * time.Millisecond) // Telegram rate-limits topic creation
	}
}

// bindExistingTopics привязывает существующие вкладки группы к каналам и
// контактам по имени — чтобы после потери topics.json не плодить дубликаты.
func (b *Bridge) bindExistingTopics() {
	existing := b.tg.ListForumTopics(topicProbeMax, topicProbeMisses)
	if len(existing) == 0 {
		return
	}
	byName := map[string]int64{}
	for name, id := range existing {
		byName[strings.ToLower(name)] = id
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for idx, name := range b.info.Channels {
		if id, ok := byName[strings.ToLower(name)]; ok {
			b.topics.bind(channelKey(idx), id)
		}
	}
	for prefix, name := range b.info.Contacts {
		if id, ok := byName[strings.ToLower(name)]; ok {
			b.topics.bind(dmKey(prefix), id)
		}
	}
	log.Printf("topics: group has %d вкладок, привязано по имени", len(existing))
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
	dmName := ""
	if m.Kind == companion.KindDM {
		dmName = t.name
	}
	return b.tg.SendMessage(formatForTelegram(m, dmName), thread)
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
	if name == "" && b.r != nil {
		// канал мог появиться после старта сессии — спросим радио
		if n, _, err := b.r.QueryChannel(m.ChannelIdx); err == nil && n != "" {
			name = n
			b.info.Channels[m.ChannelIdx] = n
		}
	}
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

// meshcorePublicPSK — стандартный ключ канала Public (base64
// "izOH6cXN6mrJ5e26oRXNcg==", как в main_companion.cpp).
var meshcorePublicPSK = [16]byte{
	0x8b, 0x33, 0x87, 0xe9, 0xc5, 0xcd, 0xea, 0x6a,
	0xc9, 0xe5, 0xed, 0xba, 0xa1, 0x15, 0xcd, 0x72,
}

// channelSecret — секрет по соглашению MeshCore-приложения:
// "#имя" — публичный: ключ выводится из имени (sha256(name)[:16]), канал
// добавляется по имени без обмена ключами; имя "Public" — стандартный
// константный ключ; иначе — случайный (приватный, ключ раздаётся).
func channelSecret(name string) [16]byte {
	var s [16]byte
	if strings.HasPrefix(name, "#") {
		sum := sha256.Sum256([]byte(name))
		copy(s[:], sum[:16])
		return s
	}
	if name == "Public" {
		return meshcorePublicPSK
	}
	if _, err := rand.Read(s[:]); err != nil {
		log.Printf("topics: secret for %q: %v", name, err)
	}
	return s
}

// normalizePublicChannel приводит ключ #-канала к выводному из имени,
// если он был создан с другим (например, со случайным).
func (b *Bridge) normalizePublicChannel(idx byte, name string, secret [16]byte) {
	if !strings.HasPrefix(name, "#") {
		return
	}
	want := channelSecret(name)
	if secret == want {
		return
	}
	if err := b.r.SetChannel(idx, name, want); err != nil {
		log.Printf("topics: normalize %q: %v", name, err)
		return
	}
	log.Printf("topics: channel %q key set to the name-derived public key", name)
	thread, _ := b.topics.thread(channelKey(idx))
	if err := b.tg.SendMessage(fmt.Sprintf("🔑 ключ канала «%s» приведён к публичному "+
		"(добавляется по имени)", name), thread); err != nil {
		log.Printf("telegram: %v", err)
	}
}

// linkTopic binds a Telegram topic to a mesh channel: reuses a channel with
// that name, or creates one on the radio (random secret, published in the
// topic so it can be shared with the channel members).
func (b *Bridge) linkTopic(threadID int64, name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.r == nil {
		log.Printf("topics: radio offline, cannot link %q", name)
		return false
	}

	// живой скан слотов: список с момента подключения мог устареть —
	// канал мог добавить кто-то ещё (приложение, второй клиент)
	idx := -1
	fresh := map[byte]string{}
	for i := 0; i < maxChannelSlots; i++ {
		n, secret, err := b.r.QueryChannel(byte(i))
		if err != nil || n == "" {
			continue
		}
		fresh[byte(i)] = n
		if strings.EqualFold(n, name) && idx < 0 {
			idx = i
			b.normalizePublicChannel(byte(i), n, secret)
		}
	}
	b.info.Channels = fresh
	if idx < 0 {
		for i := 0; i < maxChannelSlots; i++ {
			if _, taken := fresh[byte(i)]; !taken {
				idx = i
				break
			}
		}
	}
	if idx < 0 {
		log.Printf("topics: no free channel slot for %q", name)
		return false
	}

	if fresh[byte(idx)] == "" {
		secret := channelSecret(name)
		if err := b.r.SetChannel(byte(idx), name, secret); err != nil {
			log.Printf("topics: create channel %q: %v", name, err)
			return false
		}
		b.info.Channels[byte(idx)] = name
		var msg string
		if strings.HasPrefix(name, "#") || name == "Public" {
			msg = fmt.Sprintf("📡 канал «%s» создан на радио (слот %d)\n"+
				"Публичный: добавляется по имени в приложении", name, idx)
		} else {
			msg = fmt.Sprintf("📡 канал «%s» создан на радио (слот %d)\nКлюч: %x",
				name, idx, secret)
		}
		if err := b.tg.SendMessage(msg, threadID); err != nil {
			log.Printf("telegram: %v", err)
		}
		log.Printf("topics: channel %q -> idx %d created", name, idx)
	}

	b.topics.bind(channelKey(byte(idx)), threadID)
	return true
}

// linkTopicFromTG resolves an unknown topic's name over the API and links it.
func (b *Bridge) linkTopicFromTG(threadID int64) bool {
	if threadID <= 0 {
		return false
	}
	name, err := b.tg.GetForumTopic(threadID)
	if err != nil || name == "" {
		log.Printf("topics: get topic %d: %v", threadID, err)
		return false
	}
	return b.linkTopic(threadID, name)
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
		// тема создана вручную → создать/привязать mesh-канал
		if m.ForumTopicCreated != nil {
			b.linkTopic(m.MessageThreadID, m.ForumTopicCreated.Name)
			offset = u.UpdateID + 1
			continue
		}
		body := strings.TrimSpace(m.Text)
		if body == "" {
			offset = u.UpdateID + 1
			continue
		}
		tgt, ok := b.targetFromThread(m)
		if !ok {
			// тема без привязки (создана раньше бота) — создать канал по её имени
			if !b.linkTopicFromTG(m.MessageThreadID) {
				log.Printf("mesh: unknown topic (thread %d), skipping", m.MessageThreadID)
				offset = u.UpdateID + 1
				continue
			}
			tgt, ok = b.targetFromThread(m)
			if !ok {
				offset = u.UpdateID + 1
				continue
			}
		}
		budget := companion.MaxDMText
		if tgt.kind == companion.KindChannel {
			budget = companion.MaxChannelText
		}
		chunks := meshMessages(m, budget)
		if len(chunks) == 0 {
			offset = u.UpdateID + 1
			continue
		}
		var ts uint32
		var err error
		for _, chunk := range chunks {
			ts, err = b.sendTo(tgt, chunk)
			if err != nil {
				// offset остаётся: Telegram доставит сообщение повторно
				log.Printf("mesh: %v, will retry %q", err, chunk)
				return offset
			}
		}
		offset = u.UpdateID + 1
		if tgt.kind == companion.KindChannel {
			// retransmit feedback only exists for channel floods; один ответ
			// на всё сообщение — по последней части
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

// splitSender extracts the "Имя: " prefix MeshCore senders bake into channel
// texts; the name is kept as "[Имя]" so a quoted reply can address it.
func splitSender(text string) (name, rest string) {
	head := text
	if len(head) > maxNameLen+2 {
		head = head[:maxNameLen+2]
	}
	if i := strings.Index(head, ": "); i > 0 {
		return text[:i], text[i+2:]
	}
	return "", text
}

// quotedContact extracts the "[Имя]" prefix from a (possibly header-prefixed)
// message text.
func quotedContact(text string) string {
	if i := strings.LastIndexByte(text, '\n'); i >= 0 {
		text = text[i+1:]
	}
	if !strings.HasPrefix(text, "[") {
		return ""
	}
	i := strings.IndexByte(text, ']')
	if i <= 1 {
		return ""
	}
	return text[1:i]
}

// formatForTelegram renders the mesh metadata header: "18:37 · SNR 8.5 dB"
// on its own line above "[Имя] текст". A zero mesh timestamp (no clock on
// the sending node) is left out rather than shown as epoch. dmName names the
// contact for DMs, whose wire text carries no sender prefix.
func formatForTelegram(m companion.Message, dmName string) string {
	// служебные маркеры зеркал (джойстик/автоматика) в TG не показываем
	text := strings.TrimPrefix(m.Text, "(>>\u2713) ")
	text = strings.TrimPrefix(text, "(>>\u2717) ")
	text = strings.TrimPrefix(text, "(>>) ")
	name, text := splitSender(text)
	if name == "" {
		name = dmName
	}
	if name != "" {
		text = "[" + name + "] " + text
	}

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
		return text
	}
	return head + "\n" + text
}

// formatHeard renders the repeater-count reply.
func formatHeard(n int) string {
	switch {
	case n < 0:
		return "📡 нет данных о доставке"
	case n == 0:
		return "❌ никто не услышал"
	default:
		return fmt.Sprintf("📡 %s %d %s",
			plural(n, "услышал", "услышали", "услышали"),
			n,
			plural(n, "репиттер", "репиттера", "репиттеров"))
	}
}

// meshMessages renders a Telegram message into one or more mesh texts, each
// within budget bytes. No sender prefix: the radio's node_name signs every
// channel message already. A quote mention "@Имя" rides the first chunk only.
// Whitespace-only messages yield nothing.
func meshMessages(m *telegram.Message, budget int) []string {
	body := strings.TrimSpace(m.Text)
	if body == "" {
		return nil
	}
	mention := ""
	if m.ReplyToMessage != nil {
		if who := quotedContact(m.ReplyToMessage.Text); who != "" {
			// формат упоминания как в приложении MeshCore: @[Имя]
			mention = "@[" + who + "] "
		}
	}

	avail := budget - len(mention)
	if avail < 1 {
		avail = 1
	}
	chunks := splitText(body, avail)

	out := make([]string, 0, len(chunks))
	for i, ch := range chunks {
		if i == 0 {
			out = append(out, mention+ch)
		} else {
			out = append(out, ch)
		}
	}
	return out
}

// splitText cuts text into pieces of at most budget bytes, never splitting a
// rune and preferring word boundaries.
func splitText(text string, budget int) []string {
	if budget < 1 {
		budget = 1
	}
	var out []string
	for len(text) > budget {
		cut := budget
		if i := strings.LastIndexAny(text[:cut], " \n"); i > 0 {
			cut = i
		} else {
			for cut > 0 && !utf8.RuneStart(text[cut]) {
				cut--
			}
		}
		if cut < 1 {
			cut = budget
			for cut < len(text) && !utf8.RuneStart(text[cut]) {
				cut++
			}
		}
		out = append(out, text[:cut])
		text = strings.TrimLeft(text[cut:], " \n")
	}
	if text != "" {
		out = append(out, text)
	}
	return out
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
