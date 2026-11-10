package telegram

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"
	"time"
)

// tgRecorder captures Bot API calls: one entry per non-getUpdates request.
type tgRecorder struct {
	mu      sync.Mutex
	calls   []url.Values
	failN   int // fail this many calls before succeeding
	getBody string
}

func (r *tgRecorder) take() []url.Values {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]url.Values, len(r.calls))
	copy(out, r.calls)
	return out
}

func newTGFake(t *testing.T, rec *tgRecorder) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if path.Base(req.URL.Path) == "getUpdates" {
			w.Write([]byte(rec.getBody))
			return
		}
		req.ParseForm()
		rec.mu.Lock()
		rec.calls = append(rec.calls, req.PostForm)
		n := len(rec.calls)
		rec.mu.Unlock()
		if n <= rec.failN {
			w.Write([]byte(`{"ok":false,"description":"Too Many Requests"}`))
			return
		}
		w.Write([]byte(`{"ok":true,"result":{}}`))
	}))
	t.Cleanup(srv.Close)

	c := New("TEST-TOKEN", -100500)
	c.baseURL = srv.URL
	return c
}

func TestGetUpdatesParses(t *testing.T) {
	rec := &tgRecorder{getBody: `{"ok":true,"result":[
		{"update_id":7,"message":{"message_id":70,"text":"привет","message_thread_id":5,
		 "is_topic_message":true,"chat":{"id":-100500},
		 "from":{"id":42,"first_name":"Иван","last_name":"Петров","username":"iv"}}},
		{"update_id":8}
	]}`}
	c := newTGFake(t, rec)

	updates, err := c.GetUpdates(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 {
		t.Fatalf("got %d updates", len(updates))
	}
	m := updates[0].Message
	if m == nil || m.MessageID != 70 || m.Text != "привет" || m.Chat.ID != -100500 {
		t.Fatalf("bad message: %+v", m)
	}
	if m.MessageThreadID != 5 || !m.IsTopicMessage {
		t.Fatalf("topic fields lost: %+v", m)
	}
	if m.From.ID != 42 || m.From.FirstName != "Иван" || m.From.Username != "iv" {
		t.Fatalf("bad from: %+v", m.From)
	}
	if updates[1].Message != nil {
		t.Fatalf("update without message must parse as nil: %+v", updates[1])
	}
}

func TestSendMessageParams(t *testing.T) {
	rec := &tgRecorder{}
	c := newTGFake(t, rec)

	if err := c.SendMessage("hello", 0); err != nil {
		t.Fatal(err)
	}
	if err := c.SendMessage("в канал", 7); err != nil {
		t.Fatal(err)
	}
	if err := c.ReplyToMessage(7, 70, "2 репиттера услышали"); err != nil {
		t.Fatal(err)
	}

	calls := rec.take()
	if len(calls) != 3 {
		t.Fatalf("got %d calls", len(calls))
	}
	if calls[0].Get("chat_id") != "-100500" || calls[0].Get("text") != "hello" {
		t.Fatalf("sendMessage form: %v", calls[0])
	}
	if calls[0].Get("message_thread_id") != "" {
		t.Fatalf("General must not set message_thread_id: %v", calls[0])
	}
	if calls[1].Get("message_thread_id") != "7" {
		t.Fatalf("topic send must set message_thread_id: %v", calls[1])
	}
	if calls[2].Get("reply_to_message_id") != "70" || calls[2].Get("message_thread_id") != "7" {
		t.Fatalf("reply form: %v", calls[2])
	}
}

func TestCreateForumTopic(t *testing.T) {
	rec := &tgRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.ParseForm()
		rec.mu.Lock()
		rec.calls = append(rec.calls, req.PostForm)
		rec.mu.Unlock()
		w.Write([]byte(`{"ok":true,"result":{"message_thread_id":42,"name":"Public"}}`))
	}))
	t.Cleanup(srv.Close)
	c := New("TEST-TOKEN", -100500)
	c.baseURL = srv.URL

	id, err := c.CreateForumTopic("Public")
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("thread id = %d", id)
	}
	calls := rec.take()
	if len(calls) != 1 || calls[0].Get("name") != "Public" || calls[0].Get("chat_id") != "-100500" {
		t.Fatalf("form: %v", calls)
	}
}

func TestPostMessageRetriesThenSucceeds(t *testing.T) {
	old := postRetryDelay
	postRetryDelay = time.Millisecond
	defer func() { postRetryDelay = old }()

	rec := &tgRecorder{failN: 2}
	c := newTGFake(t, rec)

	if err := c.SendMessage("flaky", 0); err != nil {
		t.Fatalf("must survive two failures: %v", err)
	}
	if got := len(rec.take()); got != 3 {
		t.Fatalf("got %d attempts, want 3", got)
	}
}

func TestPostMessageGivesUp(t *testing.T) {
	old := postRetryDelay
	postRetryDelay = time.Millisecond
	defer func() { postRetryDelay = old }()

	rec := &tgRecorder{failN: 3}
	c := newTGFake(t, rec)

	err := c.SendMessage("dead", 0)
	if err == nil || !strings.Contains(err.Error(), "Too Many Requests") {
		t.Fatalf("want retryable error surfaced, got %v", err)
	}
	if got := len(rec.take()); got != 3 {
		t.Fatalf("got %d attempts, want 3", got)
	}
}
