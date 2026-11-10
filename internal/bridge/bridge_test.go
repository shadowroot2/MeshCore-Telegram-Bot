package bridge

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/companion"
	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/telegram"
)

func msg(text string, ts uint32, snr float32, hasSNR bool) companion.Message {
	return companion.Message{Kind: companion.KindChannel, Text: text, Time: ts, SNR: snr, HasSNR: hasSNR}
}

// stubRadio implements the radio interface without any network.
type stubRadio struct {
	mu            sync.Mutex
	chSent        []string // тексты канальных отправок
	dmSent        []string // тексты DM
	chNames       map[byte]string
	chSecrets     map[byte][16]byte
	setCalls      []string // имена, созданные через SetChannel
	setSecrets    [][16]byte
	sentTs        []uint32 // timestamp'ы отправок
	reply         string
	replyEr       error
	sendErr       error
	ts            uint32
	cliCalls      int // сколько раз дёргали RunCLI
	chContacts    map[[6]byte]string
	contactsCalls int
}

func (s *stubRadio) SendChannelText(idx byte, text string, ts uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// запись до ошибки: пакет уходит в эфир, теряется только ack
	s.chSent = append(s.chSent, text)
	s.sentTs = append(s.sentTs, ts)
	return s.sendErr
}

func (s *stubRadio) SendContactText(prefix [6]byte, text string, ts uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dmSent = append(s.dmSent, text)
	s.sentTs = append(s.sentTs, ts)
	return s.sendErr
}

func (s *stubRadio) QueryChannel(idx byte) (string, [16]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chNames[idx], s.chSecrets[idx], nil
}

func (s *stubRadio) SetChannel(idx byte, name string, secret [16]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chNames == nil {
		s.chNames = map[byte]string{}
	}
	if s.chSecrets == nil {
		s.chSecrets = map[byte][16]byte{}
	}
	s.chNames[idx] = name
	s.chSecrets[idx] = secret
	s.setCalls = append(s.setCalls, name)
	s.setSecrets = append(s.setSecrets, secret)
	return nil
}

func (s *stubRadio) Contacts() (map[[6]byte]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contactsCalls++
	out := map[[6]byte]string{}
	for k, v := range s.chContacts {
		out[k] = v
	}
	return out, nil
}

func (s *stubRadio) RunCLI(line string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cliCalls++
	return s.reply, s.replyEr
}

func (s *stubRadio) texts() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.chSent...), append([]string(nil), s.dmSent...)
}

type outMsg struct {
	text   string
	thread int64
}

type outReply struct {
	thread int64
	to     int64
	text   string
}

// stubTG implements the Telegram interface, recording outgoing traffic and
// minting topic ids on createForumTopic.
type stubTG struct {
	mu          sync.Mutex
	sent        []outMsg
	replies     []outReply
	created     []string
	nextID      int64
	topicFailN  int // fail this many CreateForumTopic calls first
	threadNames map[int64]string
	renames     []string
	listed      map[string]int64 // имя вкладки -> thread id для ListForumTopics
	sendFailN   int              // столько SendMessage подряд упадут с "not found"
	editErr     error
}

func (s *stubTG) ChatID() int64 { return -100500 }

func (s *stubTG) GetUpdates(offset int64) ([]telegram.Update, error) { return nil, nil }

func (s *stubTG) SendMessage(text string, threadID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendFailN > 0 {
		s.sendFailN--
		return errors.New("Bad Request: message thread not found")
	}
	s.sent = append(s.sent, outMsg{text, threadID})
	return nil
}

func (s *stubTG) ReplyToMessage(threadID, replyToMsgID int64, text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies = append(s.replies, outReply{threadID, replyToMsgID, text})
	return nil
}

func (s *stubTG) ListForumTopics(maxThreadID, maxMisses int64) map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int64{}
	for k, v := range s.listed {
		out[k] = v
	}
	return out
}

func (s *stubTG) EditForumTopic(threadID int64, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.editErr != nil {
		return s.editErr
	}
	s.renames = append(s.renames, name)
	return nil
}

func (s *stubTG) GetForumTopic(threadID int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.threadNames[threadID]
	if !ok {
		return "", errors.New("topic not found")
	}
	return name, nil
}

func (s *stubTG) CreateForumTopic(name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.topicFailN > 0 {
		s.topicFailN--
		return 0, errCreateTopic
	}
	s.created = append(s.created, name)
	s.nextID++
	return s.nextID, nil
}

var errCreateTopic = errors.New("Too Many Requests: retry after 1")

func (s *stubTG) log() ([]outMsg, []outReply, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]outMsg(nil), s.sent...), append([]outReply(nil), s.replies...),
		append([]string(nil), s.created...)
}

func newTestBridge(t *testing.T, tg *stubTG, r *stubRadio) *Bridge {
	t.Helper()
	b := New(tg, filepath.Join(t.TempDir(), "topics.json"), 5)
	b.r = r
	b.info = companion.SessionInfo{
		Channels:   map[byte]string{0: "Public", 1: "Other"},
		Contacts:   map[[6]byte]string{{0xDE, 0xAD, 0xBE, 0xEF, 0, 1}: "Валера"},
		DefaultIdx: 0,
	}
	if r != nil && r.chNames == nil {
		// радио отдаёт те же каналы, что и снапшот сессии
		r.chNames = map[byte]string{0: "Public", 1: "Other"}
	}
	return b
}

func TestTopicsPersistAcrossReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")
	tg := &stubTG{}
	t1 := New(tg, path, 5)
	id := t1.topics.ensure("ch:0", "Public", tg)
	if id == 0 {
		t.Fatal("topic id must be minted")
	}

	t2 := New(tg, path, 5)
	if got, ok := t2.topics.thread("ch:0"); !ok || got != id {
		t.Fatalf("reloaded mapping wrong: %d %v", got, ok)
	}
	if _, _, created := tg.log(); len(created) != 1 {
		t.Fatalf("reload must not create a second topic: %v", created)
	}
}

func TestDeliverRoutesToTopics(t *testing.T) {
	tg := &stubTG{}
	b := newTestBridge(t, tg, nil)

	if err := b.Deliver(msg("привет из меша", 1700000000, 8.5, true)); err != nil {
		t.Fatal(err)
	}
	dm := companion.Message{Kind: companion.KindDM,
		Prefix: [6]byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 1},
		Text:   "личка", Time: 1700000001}
	if err := b.Deliver(dm); err != nil {
		t.Fatal(err)
	}

	sent, _, created := tg.log()
	if len(sent) != 2 {
		t.Fatalf("got %d sends", len(sent))
	}
	if sent[0].thread == 0 || sent[0].thread == sent[1].thread {
		t.Fatalf("each target needs its own topic: %v", sent)
	}
	if !strings.Contains(sent[0].text, "SNR 8.5 dB") {
		t.Fatalf("mesh formatting lost: %q", sent[0].text)
	}
	if !strings.Contains(sent[1].text, "[Валера] личка") {
		t.Fatalf("DM must carry the contact name: %q", sent[1].text)
	}
	want := map[string]bool{"Public": true, "Валера": true}
	for _, name := range created {
		delete(want, name)
	}
	if len(want) != 0 {
		t.Fatalf("topics not created for %v (created %v)", want, created)
	}
}

func TestDeliverResolvesUnknownChannel(t *testing.T) {
	tg := &stubTG{}
	// канал появился после старта сессии: в info его нет, радио подскажет имя
	b := newTestBridge(t, tg, &stubRadio{chNames: map[byte]string{5: "zephcore"}})

	m := companion.Message{Kind: companion.KindChannel, ChannelIdx: 5,
		Text: "в новый канал", Time: 1}
	if err := b.Deliver(m); err != nil {
		t.Fatal(err)
	}

	sent, _, created := tg.log()
	if len(created) != 1 || created[0] != "zephcore" {
		t.Fatalf("topic name %v, want [zephcore]", created)
	}
	if sent[0].thread == 0 {
		t.Fatalf("message parked in General: %v", sent)
	}
}

func TestTopicsEnsureRetries(t *testing.T) {
	tg := &stubTG{topicFailN: 1}
	b := New(tg, filepath.Join(t.TempDir(), "topics.json"), 5)
	id := b.topics.ensure("ch:0", "Public", tg)
	if id == 0 {
		t.Fatal("ensure must retry a failed creation")
	}
}

func TestProcessUpdatesRoutesByTopic(t *testing.T) {
	oldDelay := heardCheckDelay
	heardCheckDelay = 5 * time.Millisecond
	defer func() { heardCheckDelay = oldDelay }()

	r := &stubRadio{reply: "> 2"}
	tg := &stubTG{}
	b := newTestBridge(t, tg, r)

	// поднять вкладки заранее и знать их id
	chThread := b.topics.ensure("ch:1", "Other", tg)
	dmThread := b.topics.ensure("dm:deadbeef0001", "Валера", tg)

	updates := []telegram.Update{
		{UpdateID: 1, Message: &telegram.Message{MessageID: 11, Text: "в General",
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 1, FirstName: "А"}}},
		{UpdateID: 2, Message: &telegram.Message{MessageID: 12, Text: "в канал",
			MessageThreadID: chThread, IsTopicMessage: true,
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 2, FirstName: "Б"}}},
		{UpdateID: 3, Message: &telegram.Message{MessageID: 13, Text: "в личку",
			MessageThreadID: dmThread, IsTopicMessage: true,
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 3, FirstName: "В"}}},
		{UpdateID: 4, Message: &telegram.Message{MessageID: 14, Text: "чужая вкладка",
			MessageThreadID: 999, IsTopicMessage: true,
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 4, FirstName: "Г"}}},
	}

	if next := processUpdates(b, 0, updates); next != 5 {
		t.Fatalf("offset = %d, want 5 (все обработаны, чужая вкладка — skip)", next)
	}

	ch, dm := r.texts()
	if len(ch) != 2 || ch[0] != "в General" || ch[1] != "в канал" {
		t.Fatalf("channel sends %v", ch)
	}
	if len(dm) != 1 || dm[0] != "в личку" {
		t.Fatalf("dm sends %v", dm)
	}

	// heard-count уходит обеим канальным отправкам (General и Other), DM — без него
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, replies, _ := tg.log()
		found := false
		for _, rep := range replies {
			if rep.to == 12 {
				found = true
				if rep.thread != chThread || rep.text != "📡 услышали 2 репиттера" {
					t.Fatalf("heard reply %v", rep)
				}
			}
			if rep.to == 13 {
				t.Fatalf("DM must not get a heard reply: %v", rep)
			}
		}
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no heard-count reply for message 12: %v", replies)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestProcessUpdatesKeepsOffsetWhenRadioDown(t *testing.T) {
	tg := &stubTG{}
	b := New(tg, filepath.Join(t.TempDir(), "topics.json"), 5) // no session

	updates := []telegram.Update{
		{UpdateID: 1, Message: &telegram.Message{MessageID: 11, Text: "привет",
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 4, FirstName: "Иван"}}},
	}
	if next := processUpdates(b, 0, updates); next != 0 {
		t.Fatalf("offset = %d, want 0 (message must be redelivered)", next)
	}
}

func TestCheckHeardReplyTexts(t *testing.T) {
	oldDelay := heardCheckDelay
	heardCheckDelay = time.Millisecond
	defer func() { heardCheckDelay = oldDelay }()

	r := &stubRadio{reply: "> 3"}
	tg := &stubTG{}
	b := newTestBridge(t, tg, r)

	m := &telegram.Message{MessageID: 42, MessageThreadID: 7, IsTopicMessage: true}
	checkHeard(b, m, 1700000000)

	_, replies, _ := tg.log()
	if len(replies) != 1 {
		t.Fatalf("got %d replies", len(replies))
	}
	if replies[0].to != 42 || replies[0].thread != 7 || replies[0].text != "📡 услышали 3 репиттера" {
		t.Fatalf("reply: %+v", replies[0])
	}
}

func TestHeardCountGarbageReply(t *testing.T) {
	b := newTestBridge(t, &stubTG{}, &stubRadio{reply: "Unknown command"})
	n, err := b.HeardCount(1700000000)
	if err != nil {
		t.Fatalf("HeardCount: %v", err)
	}
	if n != -1 {
		t.Fatalf("n = %d, want -1 for unparseable reply", n)
	}
}

func TestFormatForTelegram(t *testing.T) {
	m := msg("привет из меша", 1700000000, 8.5, true)
	want := time.Unix(1700000000, 0).Format("15:04") + " · SNR 8.5 dB\nпривет из меша"
	if got := formatForTelegram(m, "", time.Local); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	// wire-префикс "Имя: " превращается в "[Имя]"
	m2 := msg("ShadoW: И тебе привет", 0, 0, false)
	if got := formatForTelegram(m2, "", time.Local); got != "[ShadoW] И тебе привет" {
		t.Fatalf("got %q", got)
	}

	// DM: имя контакта подставляется, т.к. в тексте его нет
	m3 := msg("личка", 1700000000, 0, false)
	want3 := time.Unix(1700000000, 0).Format("15:04") + "\n[Валера] личка"
	if got := formatForTelegram(m3, "Валера", time.Local); got != want3 {
		t.Fatalf("got %q, want %q", got, want3)
	}

	m4 := msg("без часов", 0, 8.5, true)
	if got := formatForTelegram(m4, "", time.Local); got != "SNR 8.5 dB\nбез часов" {
		t.Fatalf("got %q", got)
	}

	// маркеры зеркал (джойстик/автоматика) вычищаются
	m5 := msg("(>>✓) ShadoW: test", 0, 0, false)
	if got := formatForTelegram(m5, "", time.Local); got != "[ShadoW] test" {
		t.Fatalf("mirror marker: %q", got)
	}
}

func TestQuotedContact(t *testing.T) {
	cases := map[string]string{
		"[Валера] привет":                     "Валера",
		"18:37 · SNR 8.5 dB\n[Валера] привет": "Валера",
		"привет":         "",
		"[Валера привет": "",
		"18:37\n[shadoW-длинный-тег] текст": "shadoW-длинный-тег",
	}
	for text, want := range cases {
		if got := quotedContact(text); got != want {
			t.Fatalf("quotedContact(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestFormatHeard(t *testing.T) {
	cases := map[int]string{
		-1: "📡 нет данных о доставке",
		0:  "❌ никто не услышал",
		1:  "📡 услышал 1 репиттер",
		2:  "📡 услышали 2 репиттера",
		5:  "📡 услышали 5 репиттеров",
		12: "📡 услышали 12 репиттеров",
		21: "📡 услышал 21 репиттер",
	}
	for n, want := range cases {
		if got := formatHeard(n); got != want {
			t.Fatalf("formatHeard(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestMeshMessages(t *testing.T) {
	m := &telegram.Message{Text: "  привет  ", From: telegram.From{FirstName: "Иван"}}
	if got := meshMessages(m, 100); len(got) != 1 || got[0] != "привет" {
		t.Fatalf("got %v", got)
	}

	m2 := &telegram.Message{Text: "   \n\t "}
	if got := meshMessages(m2, 100); got != nil {
		t.Fatalf("whitespace-only must be skipped, got %v", got)
	}

	m3 := &telegram.Message{Text: "просто текст", From: telegram.From{FirstName: strings.Repeat("Д", 40)}}
	if got := meshMessages(m3, 100); len(got) != 1 || got[0] != "просто текст" {
		t.Fatalf("nick must not leak into mesh: %v", got)
	}

	// ответ (цитата) на сообщение с [Имя] упоминает контакт через @
	m4 := &telegram.Message{Text: "да, согласен", From: telegram.From{FirstName: "Иван"},
		ReplyToMessage: &telegram.Message{Text: "18:37 · SNR 8.5 dB\n[Валера] привет"}}
	if got := meshMessages(m4, 100); len(got) != 1 || got[0] != "@[Валера] да, согласен" {
		t.Fatalf("quote mention: %v", got)
	}

	// цитата без [Имя] — просто текст
	m5 := &telegram.Message{Text: "да", From: telegram.From{FirstName: "Иван"},
		ReplyToMessage: &telegram.Message{Text: "18:37\nпросто текст"}}
	if got := meshMessages(m5, 100); len(got) != 1 || got[0] != "да" {
		t.Fatalf("plain quote: %v", got)
	}
}

func TestSplitText(t *testing.T) {
	// режем по границе слова
	parts := splitText("aaa bbb ccc", 7)
	if strings.Join(parts, "|") != "aaa|bbb ccc" {
		t.Fatalf("word split: %q", parts)
	}
	// кириллица не рвётся посередине руны
	parts = splitText("йцукенгшщз", 4)
	for _, p := range parts {
		if !utf8.ValidString(p) {
			t.Fatalf("broken rune in %q", parts)
		}
	}
	// одно длинное слово без пробелов
	parts = splitText(strings.Repeat("б", 25), 10)
	if strings.Join(parts, "") != strings.Repeat("б", 25) {
		t.Fatalf("hard split lost data: %q", parts)
	}
	for _, p := range parts {
		if len(p) > 10 {
			t.Fatalf("chunk over budget: %q", p)
		}
	}
}

func TestMeshMessagesSplitsLong(t *testing.T) {
	body := strings.Repeat("слово ", 60) // 360 байт — больше лимита фрейма
	m := &telegram.Message{Text: body, From: telegram.From{FirstName: "Иван"},
		ReplyToMessage: &telegram.Message{Text: "18:37\n[Валера] привет"}}

	chunks := meshMessages(m, companion.MaxChannelText)
	if len(chunks) < 2 {
		t.Fatalf("long message must split, got %d chunks", len(chunks))
	}
	if !strings.HasPrefix(chunks[0], "@[Валера] ") {
		t.Fatalf("first chunk must carry the mention: %q", chunks[0])
	}
	for i, ch := range chunks {
		if len(ch) > companion.MaxChannelText {
			t.Fatalf("chunk %d over budget: %d bytes", i, len(ch))
		}
		if i > 0 && strings.Contains(ch, "@[Валера]") {
			t.Fatalf("mention must ride the first chunk only: %q", ch)
		}
	}
	joined := ""
	for i, ch := range chunks {
		if i == 0 {
			ch = strings.TrimPrefix(ch, "@[Валера] ")
		}
		joined += ch + " "
	}
	if strings.Join(strings.Fields(joined), " ") != strings.Join(strings.Fields(body), " ") {
		t.Fatal("chunks must preserve the full body")
	}
}

func TestLinkTopicCreatesChannel(t *testing.T) {
	tg := &stubTG{}
	b := newTestBridge(t, tg, &stubRadio{})

	// событие создания темы вручную
	if !b.linkTopic(55, "новый-канал") {
		t.Fatal("linkTopic failed")
	}

	sent, _, _ := tg.log()
	if len(sent) != 1 || sent[0].thread != 55 {
		t.Fatalf("key message must land in the topic: %v", sent)
	}
	if !strings.Contains(sent[0].text, "новый-канал") || !strings.Contains(sent[0].text, "Ключ:") {
		t.Fatalf("key message: %q", sent[0].text)
	}

	// сообщения из этой темы теперь идут в новый канал
	updates := []telegram.Update{
		{UpdateID: 1, Message: &telegram.Message{MessageID: 11, Text: "привет",
			MessageThreadID: 55, IsTopicMessage: true,
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 1, FirstName: "А"}}},
	}
	if next := processUpdates(b, 0, updates); next != 2 {
		t.Fatalf("offset = %d", next)
	}
	ch, _ := b.r.(*stubRadio).texts()
	if len(ch) != 1 || ch[0] != "привет" {
		t.Fatalf("channel sends %v", ch)
	}
}

func TestLinkTopicReusesChannel(t *testing.T) {
	tg := &stubTG{}
	b := newTestBridge(t, tg, &stubRadio{})

	if !b.linkTopic(56, "Other") {
		t.Fatal("linkTopic failed")
	}
	if calls := b.r.(*stubRadio).setCalls; len(calls) != 0 {
		t.Fatalf("existing channel must not be recreated: %v", calls)
	}
	updates := []telegram.Update{
		{UpdateID: 1, Message: &telegram.Message{MessageID: 11, Text: "туда",
			MessageThreadID: 56, IsTopicMessage: true,
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 1, FirstName: "А"}}},
	}
	processUpdates(b, 0, updates)
	ch, _ := b.r.(*stubRadio).texts()
	if len(ch) != 1 || ch[0] != "туда" {
		t.Fatalf("channel sends %v", ch)
	}
}

func TestLinkTopicFallbackFromMessage(t *testing.T) {
	tg := &stubTG{threadNames: map[int64]string{70: "забытая"}}
	b := newTestBridge(t, tg, &stubRadio{})

	updates := []telegram.Update{
		{UpdateID: 1, Message: &telegram.Message{MessageID: 11, Text: "сообщение",
			MessageThreadID: 70, IsTopicMessage: true,
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 1, FirstName: "А"}}},
	}
	if next := processUpdates(b, 0, updates); next != 2 {
		t.Fatalf("offset = %d", next)
	}
	if calls := b.r.(*stubRadio).setCalls; len(calls) != 1 || calls[0] != "забытая" {
		t.Fatalf("channel must be created from topic name: %v", calls)
	}
	ch, _ := b.r.(*stubRadio).texts()
	if len(ch) != 1 || ch[0] != "сообщение" {
		t.Fatalf("message must reach the new channel: %v", ch)
	}
}

func TestLinkTopicFindsChannelCreatedAfterAttach(t *testing.T) {
	r := &stubRadio{chNames: map[byte]string{0: "Public", 1: "Other"}}
	tg := &stubTG{}
	b := newTestBridge(t, tg, r)

	// канал добавили после старта сессии — в снапшоте его нет
	r.mu.Lock()
	r.chNames[2] = "поздний"
	r.mu.Unlock()

	if !b.linkTopic(57, "поздний") {
		t.Fatal("linkTopic failed")
	}
	if calls := r.setCalls; len(calls) != 0 {
		t.Fatalf("existing radio channel must be reused, not recreated: %v", calls)
	}
	updates := []telegram.Update{
		{UpdateID: 1, Message: &telegram.Message{MessageID: 11, Text: "туда",
			MessageThreadID: 57, IsTopicMessage: true,
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 1, FirstName: "А"}}},
	}
	processUpdates(b, 0, updates)
	ch, _ := r.texts()
	if len(ch) != 1 || ch[0] != "туда" {
		t.Fatalf("channel sends %v", ch)
	}
}

func derivedKey(name string) [16]byte {
	var s [16]byte
	sum := sha256.Sum256([]byte(name))
	copy(s[:], sum[:16])
	return s
}

func TestLinkTopicPublicChannelDerivedKey(t *testing.T) {
	tg := &stubTG{}
	b := newTestBridge(t, tg, &stubRadio{})

	if !b.linkTopic(58, "#публичный") {
		t.Fatal("linkTopic failed")
	}
	sec := b.r.(*stubRadio).setSecrets
	if len(sec) != 1 || sec[0] != derivedKey("#публичный") {
		t.Fatalf("#-channel key must derive from the name: %x", sec)
	}
	sent, _, _ := tg.log()
	if len(sent) != 1 || !strings.Contains(sent[0].text, "Публичный") {
		t.Fatalf("public notice: %v", sent)
	}
	if strings.Contains(sent[0].text, "Ключ:") {
		t.Fatalf("public channel must not dump a key: %q", sent[0].text)
	}
}

func TestLinkTopicNormalizesWrongKey(t *testing.T) {
	// #-канал на радио с чужим ключом (создан раньше/вручную)
	r := &stubRadio{chNames: map[byte]string{0: "Public", 1: "#старый"},
		chSecrets: map[byte][16]byte{1: {0xAA, 0xBB}}}
	tg := &stubTG{}
	b := newTestBridge(t, tg, r)

	if !b.linkTopic(59, "#старый") {
		t.Fatal("linkTopic failed")
	}
	sec := r.setSecrets
	if len(sec) != 1 || sec[0] != derivedKey("#старый") {
		t.Fatalf("wrong key must be rewritten to the derived one: %x", sec)
	}
	sent, _, _ := tg.log()
	if len(sent) != 1 || !strings.Contains(sent[0].text, "приведён к публичному") {
		t.Fatalf("normalize notice: %v", sent)
	}
}

func TestLinkTopicPrivateChannelGetsRandomKey(t *testing.T) {
	tg := &stubTG{}
	b := newTestBridge(t, tg, &stubRadio{})

	if !b.linkTopic(60, "приватный") {
		t.Fatal("linkTopic failed")
	}
	sec := b.r.(*stubRadio).setSecrets
	if len(sec) != 1 || sec[0] == derivedKey("приватный") || sec[0] == meshcorePublicPSK {
		t.Fatalf("private channel must get a random key: %x", sec)
	}
	sent, _, _ := tg.log()
	if len(sent) != 1 || !strings.Contains(sent[0].text, "Ключ:") {
		t.Fatalf("private channel must publish its key: %v", sent)
	}
}

func TestSyncTopicsReusesExistingBy(t *testing.T) {
	// topics.json потерян, но вкладки в группе уже есть
	r := &stubRadio{chNames: map[byte]string{0: "Public", 1: "Other"}}
	tg := &stubTG{listed: map[string]int64{"Public": 10, "other": 12, "Валера": 14}}
	b := newTestBridge(t, tg, r)
	b.info.Contacts = map[[6]byte]string{{0xDE, 0xAD, 0xBE, 0xEF, 0, 1}: "Валера"}

	b.syncTopics()

	sent, _, created := tg.log()
	if len(created) != 0 {
		t.Fatalf("existing topics must be reused, created %v", created)
	}
	// привязка по имени (без учёта регистра) сохранилась в topics.json
	for key, want := range map[string]int64{"ch:0": 10, "ch:1": 12,
		"dm:deadbeef0001": 14} {
		if got, ok := b.topics.thread(key); !ok || got != want {
			t.Fatalf("binding %s = %d %v, want %d", key, got, ok, want)
		}
	}
	if len(sent) != 0 {
		t.Fatalf("no notices expected, got %v", sent)
	}
}

func TestSyncTopicsCreatesMissingOnly(t *testing.T) {
	r := &stubRadio{chNames: map[byte]string{0: "Public", 1: "Other"}}
	tg := &stubTG{listed: map[string]int64{"Public": 10}}
	b := newTestBridge(t, tg, r)

	b.syncTopics()

	_, _, created := tg.log()
	if len(created) != 1 || created[0] != "Other" {
		t.Fatalf("only the missing topic must be created: %v", created)
	}
}

func TestRetryReusesTimestamp(t *testing.T) {
	r := &stubRadio{}
	tg := &stubTG{}
	b := newTestBridge(t, tg, r)

	updates := []telegram.Update{
		{UpdateID: 7, Message: &telegram.Message{MessageID: 11, Text: "цитата",
			Chat: telegram.Chat{ID: -100500}, From: telegram.From{ID: 1, FirstName: "А"}}},
	}

	// первая попытка падает после отправки (ack потерян) — offset не двигается
	r.sendErr = errors.New("radio ack timeout")
	if next := processUpdates(b, 0, updates); next != 0 {
		t.Fatalf("offset = %d, want 0", next)
	}
	// повтор успешен — обязан уйти с тем же ts, чтобы mesh схлопнул дубль
	r.mu.Lock()
	r.sendErr = nil
	r.mu.Unlock()
	if next := processUpdates(b, 0, updates); next != 8 {
		t.Fatalf("offset = %d, want 8", next)
	}

	ts := r.sentTs
	if len(ts) != 2 {
		t.Fatalf("expected 2 sends (retry), got %d", len(ts))
	}
	if ts[0] != ts[1] {
		t.Fatalf("retry must reuse the timestamp: %d vs %d", ts[0], ts[1])
	}
}

func TestFormatForTelegramUsesRadioTZ(t *testing.T) {
	// 1700000000 = 2023-11-14 22:13:20 UTC; на UTC+5 это 03:13
	loc := time.FixedZone("UTC+5", 5*3600)
	want := "03:13\n[Имя] Да"
	got := formatForTelegram(companion.Message{Kind: companion.KindDM,
		Text: "Да", Time: 1700000000}, "Имя", loc)
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestLocationFromConfig(t *testing.T) {
	b := New(&stubTG{}, filepath.Join(t.TempDir(), "topics.json"), 5.5)
	_, off := time.Unix(0, 0).In(b.location()).Zone()
	if off != 5*3600+30*60 {
		t.Fatalf("offset = %d, want 5.5h", off)
	}
}

func TestDMNameResolvedFromContacts(t *testing.T) {
	prefix := [6]byte{0x02, 0x62, 0xAA, 0xBB, 0xCC, 0xDD}
	// в снапшоте контакта нет (авто-добавленный), но радио уже знает имя
	r := &stubRadio{chContacts: map[[6]byte]string{prefix: "Валера"}}
	tg := &stubTG{}
	b := newTestBridge(t, tg, r)
	b.info.Contacts = map[[6]byte]string{}

	dm := companion.Message{Kind: companion.KindDM, Prefix: prefix,
		Text: "Привет", Time: 1}
	if err := b.Deliver(dm); err != nil {
		t.Fatal(err)
	}

	sent, _, created := tg.log()
	if len(created) != 1 || created[0] != "Валера" {
		t.Fatalf("topic must be named by the contact: %v", created)
	}
	if !strings.Contains(sent[0].text, "[Валера]") {
		t.Fatalf("tag must carry the contact name: %q", sent[0].text)
	}
	if r.contactsCalls == 0 {
		t.Fatal("contact list must be re-read for an unknown sender")
	}
}

func TestDMTopicRenamedWhenNameKnown(t *testing.T) {
	prefix := [6]byte{0x02, 0x62, 0xAA, 0xBB, 0xCC, 0xDD}
	r := &stubRadio{chContacts: map[[6]byte]string{prefix: "Валера"}}
	tg := &stubTG{}
	b := newTestBridge(t, tg, r)
	// вкладка уже создана раньше с fallback-именем
	b.topics.bind(dmKey(prefix), 55, "dm-0262")

	dm := companion.Message{Kind: companion.KindDM, Prefix: prefix,
		Text: "снова", Time: 2}
	if err := b.Deliver(dm); err != nil {
		t.Fatal(err)
	}

	if len(tg.renames) != 1 || tg.renames[0] != "Валера" {
		t.Fatalf("topic must be renamed: %v", tg.renames)
	}
	sent, _, created := tg.log()
	if len(created) != 0 {
		t.Fatalf("no new topics expected: %v", created)
	}
	if len(sent) != 1 || !strings.Contains(sent[0].text, "[Валера]") {
		t.Fatalf("message must use the contact name: %v", sent)
	}
}

func TestDeliverRecreatesDeletedTopic(t *testing.T) {
	prefix := [6]byte{0x02, 0x62, 0xAA, 0xBB, 0xCC, 0xDD}
	r := &stubRadio{chContacts: map[[6]byte]string{prefix: "Валера"}}
	tg := &stubTG{sendFailN: 1}
	b := newTestBridge(t, tg, r)
	b.info.Contacts = map[[6]byte]string{prefix: "Валера"}
	// маппинг на вкладку, которую удалили в Telegram
	b.topics.bind(dmKey(prefix), 55, "Валера")

	dm := companion.Message{Kind: companion.KindDM, Prefix: prefix,
		Text: "снова", Time: 3}
	if err := b.Deliver(dm); err != nil {
		t.Fatalf("deleted topic must be recreated and resent: %v", err)
	}

	sent, _, created := tg.log()
	if len(created) != 1 || created[0] != "Валера" {
		t.Fatalf("topic must be recreated: %v", created)
	}
	if len(sent) != 1 || !strings.Contains(sent[0].text, "снова") {
		t.Fatalf("message must be resent to the new topic: %v", sent)
	}
}

func TestEnsureRecreatesWhenRenameFindsDeleted(t *testing.T) {
	tg := &stubTG{editErr: errors.New("Bad Request: message thread not found")}
	b := New(tg, filepath.Join(t.TempDir(), "topics.json"), 5)
	b.topics.bind("dm:aabbccdd", 55, "dm-aabb")

	id := b.topics.ensure("dm:aabbccdd", "Валера", tg)
	if id == 0 {
		t.Fatal("deleted topic must be recreated")
	}
	if id == 55 {
		t.Fatal("must not reuse the dead thread id")
	}
	_, _, created := tg.log()
	if len(created) != 1 || created[0] != "Валера" {
		t.Fatalf("recreated with the contact name: %v", created)
	}
}

func TestAnnounceToPublicOnce(t *testing.T) {
	tg := &stubTG{}
	r := &stubRadio{chNames: map[byte]string{0: "Public"}}
	b := newTestBridge(t, tg, r)
	b.topics.bind(channelKey(0), 10, "Public")

	info := companion.SessionInfo{
		NodeName:   "✈ ShadoW TG",
		Addr:       "192.168.1.50:5000",
		Channels:   map[byte]string{0: "Public", 1: "Other", 2: "#zephcore", 3: "#sos"},
		Contacts:   map[[6]byte]string{{1}: "Валера", {2}: "Пётр"},
		DefaultIdx: 0,
	}

	b.announce(info)
	b.announce(info) // повторно — не спамим

	sent, _, _ := tg.log()
	if len(sent) != 1 {
		t.Fatalf("want exactly one announcement, got %d", len(sent))
	}
	want := "🚀 MeshCore bot запущен и подключен к «✈ ShadoW TG» (192.168.1.50:5000) · каналов: 4 · контактов: 2"
	if sent[0].text != want {
		t.Fatalf("got %q, want %q", sent[0].text, want)
	}
	if sent[0].thread != 10 {
		t.Fatalf("announcement must go to the Public topic, got thread %d", sent[0].thread)
	}
}
