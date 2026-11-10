package bridge

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/companion"
	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/telegram"
)

func msg(text string, ts uint32, snr float32, hasSNR bool) companion.Message {
	return companion.Message{Kind: companion.KindChannel, Text: text, Time: ts, SNR: snr, HasSNR: hasSNR}
}

// stubRadio implements the radio interface without any network.
type stubRadio struct {
	mu      sync.Mutex
	chSent  []string // "idx:text"
	dmSent  []string // "prefixhex:text"
	reply   string
	replyEr error
	sendErr error
	ts      uint32
}

func (s *stubRadio) SendChannelText(idx byte, text string) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return 0, s.sendErr
	}
	s.chSent = append(s.chSent, text)
	s.ts++
	return s.ts, nil
}

func (s *stubRadio) SendContactText(prefix [6]byte, text string) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return 0, s.sendErr
	}
	s.dmSent = append(s.dmSent, text)
	s.ts++
	return s.ts, nil
}

func (s *stubRadio) RunCLI(line string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	mu      sync.Mutex
	sent    []outMsg
	replies []outReply
	created []string
	nextID  int64
}

func (s *stubTG) ChatID() int64 { return -100500 }

func (s *stubTG) GetUpdates(offset int64) ([]telegram.Update, error) { return nil, nil }

func (s *stubTG) SendMessage(text string, threadID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, outMsg{text, threadID})
	return nil
}

func (s *stubTG) ReplyToMessage(threadID, replyToMsgID int64, text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies = append(s.replies, outReply{threadID, replyToMsgID, text})
	return nil
}

func (s *stubTG) CreateForumTopic(name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = append(s.created, name)
	s.nextID++
	return s.nextID, nil
}

func (s *stubTG) log() ([]outMsg, []outReply, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]outMsg(nil), s.sent...), append([]outReply(nil), s.replies...),
		append([]string(nil), s.created...)
}

func newTestBridge(t *testing.T, tg *stubTG, r *stubRadio) *Bridge {
	t.Helper()
	b := New(tg, filepath.Join(t.TempDir(), "topics.json"))
	b.r = r
	b.info = companion.SessionInfo{
		Channels:   map[byte]string{0: "Public", 1: "Other"},
		Contacts:   map[[6]byte]string{{0xDE, 0xAD, 0xBE, 0xEF, 0, 1}: "Валера"},
		DefaultIdx: 0,
	}
	return b
}

func TestTopicsPersistAcrossReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")
	tg := &stubTG{}
	t1 := New(tg, path)
	id := t1.topics.ensure("ch:0", "Public", tg)
	if id == 0 {
		t.Fatal("topic id must be minted")
	}

	t2 := New(tg, path)
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
	want := map[string]bool{"Public": true, "Валера": true}
	for _, name := range created {
		delete(want, name)
	}
	if len(want) != 0 {
		t.Fatalf("topics not created for %v (created %v)", want, created)
	}
}

func TestProcessUpdatesRoutesByTopic(t *testing.T) {
	oldDelay := heardCheckDelay
	heardCheckDelay = 5 * time.Millisecond
	defer func() { heardCheckDelay = oldDelay }()

	r := &stubRadio{}
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
	if len(ch) != 2 || ch[0] != "А: в General" || ch[1] != "Б: в канал" {
		t.Fatalf("channel sends %v", ch)
	}
	if len(dm) != 1 || dm[0] != "В: в личку" {
		t.Fatalf("dm sends %v", dm)
	}

	// heard-count только для канального сообщения (id 12)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, replies, _ := tg.log()
		if len(replies) > 0 {
			if replies[0].to != 12 || replies[0].thread != chThread {
				t.Fatalf("heard reply %v", replies[0])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no heard-count reply arrived")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestProcessUpdatesKeepsOffsetWhenRadioDown(t *testing.T) {
	tg := &stubTG{}
	b := New(tg, filepath.Join(t.TempDir(), "topics.json")) // no session

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
	if replies[0].to != 42 || replies[0].thread != 7 || replies[0].text != "3 репиттера услышали" {
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
	if got := formatForTelegram(m); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	m2 := msg("без SNR", 1700000000, 0, false)
	want2 := time.Unix(1700000000, 0).Format("15:04") + "\nбез SNR"
	if got := formatForTelegram(m2); got != want2 {
		t.Fatalf("got %q, want %q", got, want2)
	}

	m3 := msg("без часов", 0, 8.5, true)
	if got := formatForTelegram(m3); got != "SNR 8.5 dB\nбез часов" {
		t.Fatalf("got %q", got)
	}

	m4 := msg("голый текст", 0, 0, false)
	if got := formatForTelegram(m4); got != "голый текст" {
		t.Fatalf("got %q", got)
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

func TestMeshText(t *testing.T) {
	m := &telegram.Message{Text: "  привет  ", From: telegram.From{FirstName: "Иван"}}
	if got := meshText(m); got != "Иван: привет" {
		t.Fatalf("got %q", got)
	}

	m2 := &telegram.Message{Text: "   \n\t "}
	if got := meshText(m2); got != "" {
		t.Fatalf("whitespace-only must be skipped, got %q", got)
	}

	m3 := &telegram.Message{Text: "текст", From: telegram.From{FirstName: strings.Repeat("Д", 40)}}
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
	m := &telegram.Message{From: telegram.From{ID: 42, FirstName: "Иван", LastName: "Петров", Username: "iv"}}
	if got := senderName(m); got != "Иван Петров" {
		t.Fatalf("got %q", got)
	}

	m2 := &telegram.Message{From: telegram.From{Username: "iv"}}
	if got := senderName(m2); got != "@iv" {
		t.Fatalf("got %q", got)
	}

	m3 := &telegram.Message{From: telegram.From{ID: 42}}
	if got := senderName(m3); got != "id 42" {
		t.Fatalf("got %q", got)
	}
}
