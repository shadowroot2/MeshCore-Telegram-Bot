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
	"path/filepath"
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
	SendChannelText(idx byte, text string, ts uint32) error
	SendContactText(prefix [6]byte, text string, ts uint32) error
	QueryChannel(idx byte) (string, [16]byte, error)
	SetChannel(idx byte, name string, secret [16]byte) error
	Contacts() (map[[6]byte]string, error)
	RunCLI(line string) (string, error)
}

// Telegram is the Bot API surface the bridge needs; *telegram.Client
// implements it.
type Telegram interface {
	ChatID() int64
	GetUpdates(offset int64) ([]telegram.Update, error)
	SendMessage(text string, threadID int64) (int64, error)
	ReplyToMessage(threadID, replyToMsgID int64, text string) error
	PostInChat(chatID, threadID, replyTo int64, text string) (int64, error)
	IsGroupMember(userID int64) (bool, error)
	CreateForumTopic(name string) (int64, error)
	EditForumTopic(threadID int64, name string) error
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

// admins — постоянный список контактов, которым разрешены команды из LoRa.
type admins struct {
	mu    sync.Mutex
	path  string
	state map[string]string // hex префикса -> "allow"/"deny"
}

func loadAdmins(path string) *admins {
	a := &admins{path: path, state: map[string]string{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return a
	}
	if err := json.Unmarshal(data, &a.state); err != nil {
		log.Printf("admins: %s: %v", path, err)
		a.state = map[string]string{}
	}
	log.Printf("admins: %s: %d записей", path, len(a.state))
	return a
}

func (a *admins) get(prefix [6]byte) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state[fmt.Sprintf("%x", prefix)]
}

func (a *admins) set(prefix [6]byte, state string) {
	// запись под мьютексом: конкурентные set не должны терять чужие записи
	// при копировании снимка вне блокировки
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state[fmt.Sprintf("%x", prefix)] = state
	raw := make(map[string]string, len(a.state))
	for k, v := range a.state {
		raw[k] = v
	}
	if data, err := json.Marshal(raw); err == nil {
		if err := os.WriteFile(a.path, data, 0o600); err != nil {
			log.Printf("admins: save %s: %v", a.path, err)
		}
	}
}

// pendingApproval — отложенная команда из LoRa, ждущая согласования.
type pendingApproval struct {
	cmd          string
	name         string
	prefix       [6]byte
	promptChat   int64
	promptMsg    int64
	promptThread int64
	thread       int64 // вкладка ЛС: куда цитировать результат
	forwardedID  int64 // id пересланного "$ команда" в группе
}

// topics maps mesh endpoints to Telegram forum topic ids and persists the
// mapping so restarts reuse topics instead of creating duplicates.
type topicEntry struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type topics struct {
	mu       sync.Mutex
	ensureMu sync.Mutex // сериализует создание вкладок (check-and-create)
	path     string
	byKey    map[string]topicEntry
	byID     map[int64]string
}

func loadTopics(path string) *topics {
	t := &topics{path: path, byKey: map[string]topicEntry{}, byID: map[int64]string{}}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("topics: %s: %v (start empty — все темы будут созданы заново)", path, err)
		return t
	}
	var raw map[string]topicEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		// старый формат: {"ch:0": 10} — имя вкладки неизвестно
		var legacy map[string]int64
		if err2 := json.Unmarshal(data, &legacy); err2 != nil {
			log.Printf("topics: %s: %v", path, err)
			return t
		}
		for k, id := range legacy {
			t.byKey[k] = topicEntry{ID: id}
			t.byID[id] = k
		}
	} else {
		for k, e := range raw {
			t.byKey[k] = e
			t.byID[e.ID] = k
		}
	}
	log.Printf("topics: %s: %d mappings loaded", path, len(t.byKey))
	return t
}

func (t *topics) snapshot() map[string]topicEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]topicEntry, len(t.byKey))
	for k, e := range t.byKey {
		out[k] = e
	}
	return out
}

func (t *topics) thread(key string) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.byKey[key]
	return e.ID, ok
}

func (t *topics) key(thread int64) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.byID[thread]
	return k, ok
}

// isNotFound — Telegram отвечает так на обращение к удалённой вкладке.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "not_found")
}

// invalidate выбрасывает маппинг удалённой вкладки.
func (t *topics) invalidate(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.byKey[key]; ok {
		delete(t.byID, e.ID)
		delete(t.byKey, key)
	}
	t.saveLocked()
}

func (t *topics) bind(key string, thread int64, name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byKey[key] = topicEntry{ID: thread, Name: name}
	t.byID[thread] = key
	t.saveLocked()
}

// saveLocked пишет topics.json; зовётся с t.mu, чтобы конкурентные bind/
// invalidate не перетирали файл чужими снимками.
func (t *topics) saveLocked() {
	raw := make(map[string]topicEntry, len(t.byKey))
	for k, e := range t.byKey {
		raw[k] = e
	}
	data, err := json.Marshal(raw)
	if err == nil {
		err = os.WriteFile(t.path, data, 0o600)
	}
	if err != nil {
		log.Printf("topics: save %s: %v (маппинг не переживёт перезапуск!)", t.path, err)
	}
}

// isFallbackName — имена, придуманные самим ботом; только их он имеет право
// переименовывать (dm-xxxx до узнавания контакта, channel-N). Вкладки,
// названные вручную, бот не трогает.
func isFallbackName(name string) bool {
	return strings.HasPrefix(name, "dm-") || strings.HasPrefix(name, "channel-")
}

// ensure returns the topic for key, creating it once; 0 means General.
// Creation is retried: Telegram rate-limits topic creation, and a failed
// attempt would otherwise park the whole topic in General forever. If the
// вкладка уже существует под другим именем (например dm-xxxx до того, как
// стал известен контакт) — она переименовывается.
func (t *topics) ensure(key, name string, tg Telegram) int64 {
	// check-and-create под общим замком: два потока не должны создать
	// две вкладки на один ключ
	t.ensureMu.Lock()
	defer t.ensureMu.Unlock()
	t.mu.Lock()
	e, ok := t.byKey[key]
	t.mu.Unlock()
	if ok && (e.Name == name || !isFallbackName(e.Name)) {
		return e.ID
	}
	if ok {
		err := tg.EditForumTopic(e.ID, name)
		if err == nil {
			log.Printf("topics: %q renamed to %q", e.Name, name)
			t.bind(key, e.ID, name)
			return e.ID
		}
		if !isNotFound(err) {
			log.Printf("topics: rename %q -> %q: %v", e.Name, name, err)
			return e.ID
		}
		log.Printf("topics: %q was deleted in Telegram, creating anew", e.Name)
		t.invalidate(key)
	}
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		var id int64
		id, err = tg.CreateForumTopic(name)
		if err == nil {
			t.bind(key, id, name)
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

	tsMu    sync.Mutex
	lastTs  uint32
	retryTs map[int64][]uint32 // стабильные ts для повторных отправок

	tz        *time.Location // пояс из TZ_OFFSET конфига
	announced bool           // стартовое сообщение уже ушло

	adminID int64
	admins  *admins
	pMu     sync.Mutex
	pending map[string]pendingApproval // hex префикса -> заявка
}

func New(tg Telegram, topicsFile string, tzHours float64, adminID int64) *Bridge {
	// TZ_OFFSET=0 — это UTC, а не «взять локальный»: конфиг всегда даёт
	// явный сдвиг (по умолчанию +5)
	loc := time.FixedZone(fmt.Sprintf("UTC%+g", tzHours), int(tzHours*3600))
	return &Bridge{
		tg:      tg,
		topics:  loadTopics(topicsFile),
		admins:  loadAdmins(filepath.Join(filepath.Dir(topicsFile), "admins.json")),
		tz:      loc,
		adminID: adminID,
		pending: map[string]pendingApproval{},
	}
}

// Attach implements companion.Hooks: remembers the session and syncs the
// topics with the group.
func (b *Bridge) Attach(c *companion.Client, info companion.SessionInfo) {
	b.mu.Lock()
	b.r, b.info = c, info
	b.mu.Unlock()

	b.syncTopics()
	b.announce(info)
}

// announce — приветствие в вкладку Public при первом подключении (только
// в Telegram; в mesh ничего не уходит).
func (b *Bridge) announce(info companion.SessionInfo) {
	b.mu.Lock()
	if b.announced {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	thread := int64(0) // General как запасной вариант
	for idx, name := range info.Channels {
		if name == "Public" {
			if id, ok := b.topics.thread(channelKey(idx)); ok {
				thread = id
			}
		}
	}
	text := fmt.Sprintf("🚀 MeshCore bot запущен и подключен к «%s» (%s) · каналов: %d · контактов: %d",
		info.NodeName, info.Addr, len(info.Channels), len(info.Contacts))
	if _, err := b.tg.SendMessage(text, thread); err != nil {
		// не помечаем отправленным — попробуем при следующем подключении
		log.Printf("telegram: %v", err)
		return
	}
	b.mu.Lock()
	b.announced = true
	b.mu.Unlock()
}

// location — пояс для шапок сообщений (TZ_OFFSET из конфига).
func (b *Bridge) location() *time.Location {
	return b.tz
}

// syncTopics: существующие вкладки привязываются по имени, недостающие —
// создаются; ключи #-каналов приводятся к выводному из имени.
func (b *Bridge) syncTopics() {
	b.bindExistingTopics()

	b.mu.Lock()
	r := b.r
	channels := make(map[byte]string, len(b.info.Channels))
	for idx, name := range b.info.Channels {
		channels[idx] = name
	}
	b.mu.Unlock()
	if r == nil {
		return
	}

	for idx, name := range channels {
		if _, secret, err := r.QueryChannel(idx); err == nil {
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
	byID := map[int64]string{}
	for name, id := range existing {
		byName[strings.ToLower(name)] = id
		byID[id] = name
	}

	// сперва по сохранённому id: вкладку могли переименовать вручную в TG,
	// её имя принимаем как есть — бот его не перетирает
	for key, e := range b.topics.snapshot() {
		if tgName, ok := byID[e.ID]; ok && tgName != e.Name {
			log.Printf("topics: %q renamed in Telegram to %q, following", e.Name, tgName)
			b.topics.bind(key, e.ID, tgName)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for idx, name := range b.info.Channels {
		if _, known := b.topics.thread(channelKey(idx)); known {
			continue
		}
		if id, ok := byName[strings.ToLower(name)]; ok {
			b.topics.bind(channelKey(idx), id, name)
		}
	}
	for prefix, name := range b.info.Contacts {
		if _, known := b.topics.thread(dmKey(prefix)); known {
			continue
		}
		if id, ok := byName[strings.ToLower(name)]; ok {
			b.topics.bind(dmKey(prefix), id, name)
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
	text := formatForTelegram(m, b.location())
	msgID, err := b.tg.SendMessage(text, thread)
	if isNotFound(err) {
		// вкладку удалили в Telegram — маппинг протух, создаём заново
		log.Printf("topics: %q was deleted in Telegram, recreating", t.name)
		b.topics.invalidate(t.key)
		thread = b.topics.ensure(t.key, t.name, b.tg)
		msgID, err = b.tg.SendMessage(text, thread)
	}
	if err != nil {
		return err
	}
	// "$ команда" из LoRa в личном сообщении — выполнить на радио;
	// результат вернётся цитатой в TG и DM отправителю
	if m.Kind == companion.KindDM {
		if cmd := cliFrom(m.Text); cmd != "" {
			b.startMeshCLI(m, cmd, msgID, thread)
		}
	}
	return nil
}

// cliFrom вырезает команду из текста с префиксом "$ ".
func cliFrom(text string) string {
	t := strings.TrimSpace(text)
	if !strings.HasPrefix(t, "$") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(t, "$"))
}

// contactName возвращает имя контакта; для неизвестного/безымянного префикса
// перечитывает список контактов радио — авто-добавленные узнают имена из
// advert'ов не сразу, и снапшот со старта сессии их ещё не содержит.
func (b *Bridge) contactName(prefix [6]byte) string {
	b.mu.Lock()
	if n := b.info.Contacts[prefix]; n != "" {
		b.mu.Unlock()
		return n
	}
	r := b.r
	b.mu.Unlock()
	if r == nil {
		return ""
	}
	list, err := r.Contacts()
	if err != nil {
		log.Printf("contacts: %v", err)
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for p, n := range list {
		b.info.Contacts[p] = n
	}
	return b.info.Contacts[prefix]
}

func (b *Bridge) targetFor(m companion.Message) target {
	if m.Kind == companion.KindDM {
		name := b.contactName(m.Prefix)
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
	b.mu.Lock()
	name := b.info.Channels[m.ChannelIdx]
	r := b.r
	b.mu.Unlock()
	if name == "" && r != nil {
		// канал мог появиться после старта сессии — спросим радио
		if n, _, err := r.QueryChannel(m.ChannelIdx); err == nil && n != "" {
			name = n
			b.mu.Lock()
			b.info.Channels[m.ChannelIdx] = n
			b.mu.Unlock()
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
	b.mu.Lock()
	r := b.r
	b.mu.Unlock()
	if r == nil {
		return
	}
	if err := r.SetChannel(idx, name, want); err != nil {
		log.Printf("topics: normalize %q: %v", name, err)
		return
	}
	log.Printf("topics: channel %q key set to the name-derived public key", name)
	thread, _ := b.topics.thread(channelKey(idx))
	if _, err := b.tg.SendMessage(fmt.Sprintf("🔑 ключ канала «%s» приведён к публичному "+
		"(добавляется по имени)", name), thread); err != nil {
		log.Printf("telegram: %v", err)
	}
}

// linkTopic binds a Telegram topic to a mesh channel: reuses a channel with
// that name, or creates one on the radio (random secret, published in the
// topic so it can be shared with the channel members).
func (b *Bridge) linkTopic(threadID int64, name string) bool {
	b.mu.Lock()
	r := b.r
	b.mu.Unlock()
	if r == nil {
		log.Printf("topics: radio offline, cannot link %q", name)
		return false
	}

	// живой скан слотов: список с момента подключения мог устареть —
	// канал мог добавить кто-то ещё (приложение, второй клиент)
	idx := -1
	fresh := map[byte]string{}
	for i := 0; i < maxChannelSlots; i++ {
		n, secret, err := r.QueryChannel(byte(i))
		if err != nil || n == "" {
			continue
		}
		fresh[byte(i)] = n
		if strings.EqualFold(n, name) && idx < 0 {
			idx = i
			b.normalizePublicChannel(byte(i), n, secret)
		}
	}
	b.mu.Lock()
	b.info.Channels = fresh
	b.mu.Unlock()
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
		if err := r.SetChannel(byte(idx), name, secret); err != nil {
			log.Printf("topics: create channel %q: %v", name, err)
			return false
		}
		b.mu.Lock()
		b.info.Channels[byte(idx)] = name
		b.mu.Unlock()
		var msg string
		if strings.HasPrefix(name, "#") || name == "Public" {
			msg = fmt.Sprintf("📡 канал «%s» создан на радио (слот %d)\n"+
				"Публичный: добавляется по имени в приложении", name, idx)
		} else {
			msg = fmt.Sprintf("📡 канал «%s» создан на радио (слот %d)\nКлюч: %x",
				name, idx, secret)
		}
		if _, err := b.tg.SendMessage(msg, threadID); err != nil {
			log.Printf("telegram: %v", err)
		}
		log.Printf("topics: channel %q -> idx %d created", name, idx)
	}

	b.topics.bind(channelKey(byte(idx)), threadID, name)
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

func (b *Bridge) sendTo(t target, text string, ts uint32) error {
	b.mu.Lock()
	r := b.r
	b.mu.Unlock()
	if r == nil {
		return errors.New("radio offline")
	}
	// клиент радио потокобезопасен, долгий I/O не держит b.mu — иначе
	// встречный поток (mesh->TG) стоит по 15 секунд на каждый запрос
	if t.kind == companion.KindDM {
		return r.SendContactText(t.prefix, text, ts)
	}
	return r.SendChannelText(t.channel, text, ts)
}

// isPermanentRefusal — радио отклонило запрос безнадёжно: повтор байт-в-байт
// того же пакета результата не даст. Временные отказы (таблица полна) и
// сетевые ошибки сюда не попадают — их лечит повтор.
func isPermanentRefusal(err error) bool {
	var ref *companion.RefusedError
	return errors.As(err, &ref) && ref.Permanent()
}

// tssFor выдаёт стабильные timestamp'ы для сообщения: при повторе (offset не
// сдвинулся) текст уходит с теми же ts — пакет байт-в-байт тот же, и mesh
// дедуплицирует его, вместо дублей у получателей.
func (b *Bridge) tssFor(id int64, n int) []uint32 {
	b.tsMu.Lock()
	defer b.tsMu.Unlock()
	if b.retryTs == nil {
		b.retryTs = map[int64][]uint32{}
	}
	tss := b.retryTs[id]
	for len(tss) < n {
		tss = append(tss, b.nextTsLocked())
	}
	b.retryTs[id] = tss
	return tss[:n]
}

func (b *Bridge) nextTsLocked() uint32 {
	ts := uint32(time.Now().Unix())
	if ts <= b.lastTs {
		ts = b.lastTs + 1
	}
	b.lastTs = ts
	return ts
}

func (b *Bridge) tssDone(id int64) {
	b.tsMu.Lock()
	delete(b.retryTs, id)
	b.tsMu.Unlock()
}

// HeardCount asks the radio how many neighbours repeated the flood for the
// send with that message timestamp (CLI `send.heard <ts>`). An unparseable
// reply (e.g. a firmware without `send.heard`) is reported as -1.
func (b *Bridge) HeardCount(ts uint32) (int, error) {
	b.mu.Lock()
	r := b.r
	b.mu.Unlock()
	if r == nil {
		return 0, errors.New("radio offline")
	}
	reply, err := r.RunCLI(fmt.Sprintf("send.heard %d", ts))
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
		if m == nil || m.From.IsBot {
			offset = u.UpdateID + 1
			continue
		}
		// личка бота (любой): команды "$"
		if m.Chat.Type == "private" {
			if cmd := cliFrom(strings.TrimSpace(m.Text)); cmd != "" {
				if !b.canCommand(m.From.ID) {
					b.replyTo(m, "❌ нет доступа к командам")
				} else {
					b.replyTo(m, b.execCLI(cmd))
				}
			}
			offset = u.UpdateID + 1
			continue
		}
		if m.Chat.ID != b.tg.ChatID() {
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
		// ответ на заявку «Разрешить контакту...» из вкладки ЛС
		if b.tryApproval(m) {
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
		// "$ команда" в личных чатах (вкладка ЛС) — CLI радио, в LoRa ничего
		// не уходит; в канале — обычный текст для mesh
		if cmd := cliFrom(body); cmd != "" && tgt.kind == companion.KindDM {
			if !b.canCommand(m.From.ID) {
				b.replyTo(m, "❌ нет доступа к командам")
			} else {
				b.replyTo(m, b.execCLI(cmd))
			}
			offset = u.UpdateID + 1
			continue
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
		tss := b.tssFor(u.UpdateID, len(chunks))
		for i, chunk := range chunks {
			err := b.sendTo(tgt, chunk, tss[i])
			if isPermanentRefusal(err) {
				// радио само отказалось (нет контакта, чужой слот): повтор
				// того же запроса бесполезен — пропускаем, чтобы не вешать
				// очередь TG->mesh навсегда
				log.Printf("mesh: %v, dropping %q", err, chunk)
				continue
			}
			if err != nil {
				// offset остаётся: Telegram доставит сообщение повторно,
				// а повтор пойдёт с теми же ts — mesh схлопнет дубли
				log.Printf("mesh: %v, will retry %q", err, chunk)
				return offset
			}
		}
		b.tssDone(u.UpdateID)
		offset = u.UpdateID + 1
		if tgt.kind == companion.KindChannel {
			// retransmit feedback only exists for channel floods; один ответ
			// на всё сообщение — по последней части
			go checkHeard(b, m, tss[len(tss)-1])
		}
	}
	return offset
}

// cliResult — текст ответа на команду CLI.
func cliResult(out string, err error) string {
	switch {
	case err != nil:
		return "❌ " + err.Error()
	case strings.TrimSpace(out) == "":
		return "✓ выполнено"
	default:
		return out
	}
}

// runMeshCLI выполняет разрешённую команду из LoRa: результат уходит
// цитированием в TG (на пересланную команду) и DM отправителю по LoRa.
func (b *Bridge) runMeshCLI(prefix [6]byte, cmd string, forwardedID, thread int64) {
	text := b.execCLI(cmd)
	for _, part := range splitText(text, 4000) {
		if err := b.tg.ReplyToMessage(thread, forwardedID, part); err != nil {
			log.Printf("telegram: %v", err)
		}
	}
	b.sendDM(prefix, text)
}

// startMeshCLI — команда "$" из LoRa: сразу выполнить (контакту разрешено),
// отказать (запрещено) или запросить согласование в личке.
func (b *Bridge) startMeshCLI(m companion.Message, cmd string, forwardedID, thread int64) {
	name := b.contactName(m.Prefix)
	if name == "" {
		name = "dm-" + fmt.Sprintf("%x", m.Prefix)[:4]
	}
	log.Printf("cli: $ %s от %s (из LoRa)", cmd, name)
	switch b.admins.get(m.Prefix) {
	case "allow":
		go b.runMeshCLI(m.Prefix, cmd, forwardedID, thread)
	case "deny":
		b.sendDM(m.Prefix, "❌ Вам запрещено выполнять команды")
	default:
		chat, pThread := b.promptTarget(thread)
		id, err := b.tg.PostInChat(chat, pThread, 0, approvalPrompt(name))
		if err != nil {
			log.Printf("telegram: заявка не ушла: %v", err)
			return
		}
		log.Printf("cli: заявка на $ %s отправлена в вкладку ЛС (%s)", cmd, name)
		b.setPending(m.Prefix, pendingApproval{
			cmd: cmd, name: name, prefix: m.Prefix,
			promptChat: chat, promptMsg: id, promptThread: pThread,
			thread: thread, forwardedID: forwardedID,
		})
	}
}

// promptTarget: заявка всегда живёт в вкладке ЛС контакта — весь диалог
// (вопрос, «да», результат) идёт в рамках личного сообщения, без лички бота.
func (b *Bridge) promptTarget(thread int64) (chatID, threadID int64) {
	return b.tg.ChatID(), thread
}

// sendDM отвечает отправителю личным сообщением по LoRa.
func (b *Bridge) sendDM(prefix [6]byte, text string) {
	b.mu.Lock()
	r := b.r
	b.mu.Unlock()
	if r == nil {
		return
	}
	for _, part := range splitText(text, companion.MaxDMText) {
		ts := b.nextTs()
		if err := r.SendContactText(prefix, part, ts); err != nil {
			log.Printf("cli: ответ в ЛС: %v", err)
			return
		}
	}
}

// nextTs — очередной timestamp для разовой отправки.
func (b *Bridge) nextTs() uint32 {
	b.tsMu.Lock()
	defer b.tsMu.Unlock()
	return b.nextTsLocked()
}

// canCommand — имеет ли пользователь право на команды "$" из Telegram:
// ADMIN_ID, а если не задан — любой участник группы.
func (b *Bridge) canCommand(userID int64) bool {
	if b.adminID != 0 {
		return userID == b.adminID
	}
	ok, err := b.tg.IsGroupMember(userID)
	if err != nil {
		log.Printf("admin: проверка участия: %v", err)
		return false
	}
	return ok
}

// execCLI выполняет команду CLI на радио и возвращает готовый текст ответа.
func (b *Bridge) execCLI(cmd string) string {
	b.mu.Lock()
	r := b.r
	b.mu.Unlock()

	out := ""
	var err error
	if r == nil {
		err = errors.New("радио не подключено")
	} else {
		log.Printf("cli: $ %s", cmd)
		out, err = r.RunCLI(cmd)
	}
	return cliResult(out, err)
}

// replyTo отвечает цитированием на сообщение в его чате/вкладке.
func (b *Bridge) replyTo(m *telegram.Message, text string) {
	for _, part := range splitText(text, 4000) {
		if _, err := b.tg.PostInChat(m.Chat.ID, m.MessageThreadID, m.MessageID, part); err != nil {
			log.Printf("telegram: %v", err)
		}
	}
}

// tryApproval обрабатывает ответ на заявку «Разрешить контакту ...»; true —
// сообщение было ответом на заявку (и уже обработано).
func (b *Bridge) tryApproval(m *telegram.Message) bool {
	if m.ReplyToMessage == nil {
		return false
	}
	prefix, ok := b.pendingPrefix(m.ReplyToMessage.MessageID, m.ReplyToMessage.Text)
	if !ok {
		return false
	}
	if b.adminID != 0 && m.From.ID != b.adminID {
		return true // ответ не от админа — игнорируем
	}
	b.decideApproval(m, prefix)
	return true
}

// decideApproval — «да»/«yes» сохраняет контакт как админа и выполняет
// отложенную команду, любой другой ответ запрещает.
func (b *Bridge) decideApproval(m *telegram.Message, prefix [6]byte) {
	name := b.contactName(prefix)
	if name == "" {
		name = "dm-" + fmt.Sprintf("%x", prefix)[:4]
	}
	if ans := strings.ToLower(strings.TrimSpace(m.Text)); ans == "да" || ans == "yes" {
		b.admins.set(prefix, "allow")
		b.replyTo(m, fmt.Sprintf("💾 %s сохранен как администратор!", name))
		if p, ok := b.takePending(prefix); ok {
			// результат — как для обычной команды: цитата на "$ команда"
			// и ответное ЛС отправителю по LoRa (без дубля к заявке)
			go b.runMeshCLI(p.prefix, p.cmd, p.forwardedID, p.thread)
		}
		return
	}
	b.admins.set(prefix, "deny")
	b.replyTo(m, fmt.Sprintf("❌ %s запрещено выполнять команды", name))
	b.sendDM(prefix, "❌ Вам запрещено выполнять команды")
	b.dropPending(prefix)
}

func (b *Bridge) setPending(prefix [6]byte, p pendingApproval) {
	b.pMu.Lock()
	defer b.pMu.Unlock()
	b.pending[fmt.Sprintf("%x", prefix)] = p
}

func (b *Bridge) takePending(prefix [6]byte) (pendingApproval, bool) {
	b.pMu.Lock()
	defer b.pMu.Unlock()
	k := fmt.Sprintf("%x", prefix)
	p, ok := b.pending[k]
	delete(b.pending, k)
	return p, ok
}

func (b *Bridge) dropPending(prefix [6]byte) {
	b.pMu.Lock()
	defer b.pMu.Unlock()
	delete(b.pending, fmt.Sprintf("%x", prefix))
}

// approvalPrompt — заявка на разрешение команд от контакта.
func approvalPrompt(name string) string {
	return fmt.Sprintf("🔑 Разрешить контакту %s выполнять команды?", name)
}

// promptName вытаскивает имя контакта из текста заявки.
func promptName(text string) string {
	const head = "Разрешить контакту "
	const tail = " выполнять команды?"
	if !strings.HasPrefix(text, head) || !strings.HasSuffix(text, tail) {
		return ""
	}
	return strings.TrimSpace(text[len(head) : len(text)-len(tail)])
}

// pendingPrefix находит контакт по сообщению-заявке: сначала по id (заявка
// жива в памяти), иначе по имени из текста — фолбэк после рестарта бота.
func (b *Bridge) pendingPrefix(promptMsgID int64, promptText string) ([6]byte, bool) {
	var prefix [6]byte
	b.pMu.Lock()
	for _, p := range b.pending {
		if p.promptMsg == promptMsgID {
			prefix = p.prefix
			b.pMu.Unlock()
			return prefix, true
		}
	}
	b.pMu.Unlock()

	name := promptName(promptText)
	if name == "" {
		return prefix, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for pfx, n := range b.info.Contacts {
		if n == name {
			return pfx, true
		}
	}
	return prefix, false
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

// quotedContact extracts the "[Имя]" prefix from a quoted message text. The
// tag rides the first body line: line 2 after the metadata header, line 1
// without it — многострочное сообщение не должно терять упоминание.
func quotedContact(text string) string {
	lines := strings.SplitN(text, "\n", 3)
	if len(lines) > 2 {
		lines = lines[:2]
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "[") {
			if i := strings.IndexByte(line, ']'); i > 1 {
				return line[1:i]
			}
		}
	}
	return ""
}

// formatForTelegram renders the mesh metadata header: "18:37 · SNR 8.5 dB"
// on its own line above the text. A zero mesh timestamp (no clock on the
// sending node) is left out rather than shown as epoch. Канальные сообщения
// получают тег "[Имя]" (в вкладке несколько отправителей); в личных он не
// нужен — имя контакта уже в названии вкладки.
func formatForTelegram(m companion.Message, loc *time.Location) string {
	// служебные маркеры зеркал (джойстик/автоматика) в TG не показываем
	text := strings.TrimPrefix(m.Text, "(>>\u2713) ")
	text = strings.TrimPrefix(text, "(>>\u2717) ")
	text = strings.TrimPrefix(text, "(>>) ")
	name, text := splitSender(text)
	if name != "" && m.Kind != companion.KindDM {
		text = "[" + name + "] " + text
	}

	head := ""
	if m.Time > 0 {
		head = time.Unix(int64(m.Time), 0).In(loc).Format("15:04")
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
