package telegram

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	token   string
	chatID  int64
	baseURL string
	client  *http.Client
}

func New(token string, chatID int64) *Client {
	return &Client{
		token:   token,
		chatID:  chatID,
		baseURL: "https://api.telegram.org",
		client:  &http.Client{Timeout: 65 * time.Second}, // > getUpdates long-poll
	}
}

func (c *Client) ChatID() int64 { return c.chatID }

type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

type Message struct {
	MessageID       int64  `json:"message_id"`
	MessageThreadID int64  `json:"message_thread_id"`
	IsTopicMessage  bool   `json:"is_topic_message"`
	Text            string `json:"text"`
	Chat            Chat   `json:"chat"`
	From            From   `json:"from"`
}

type Chat struct {
	ID int64 `json:"id"`
}

type From struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
}

// SendMessage posts text into the chat; threadID > 0 routes it into a forum
// topic (вкладка), 0 is the General topic.
func (c *Client) SendMessage(text string, threadID int64) error {
	v := url.Values{
		"chat_id": {strconv.FormatInt(c.chatID, 10)},
		"text":    {text},
	}
	if threadID > 0 {
		v.Set("message_thread_id", strconv.FormatInt(threadID, 10))
	}
	return c.postMessage(v)
}

func (c *Client) ReplyToMessage(threadID, replyToMsgID int64, text string) error {
	v := url.Values{
		"chat_id":             {strconv.FormatInt(c.chatID, 10)},
		"reply_to_message_id": {strconv.FormatInt(replyToMsgID, 10)},
		"text":                {text},
	}
	if threadID > 0 {
		v.Set("message_thread_id", strconv.FormatInt(threadID, 10))
	}
	return c.postMessage(v)
}

// CreateForumTopic makes a new вкладка in the forum group and returns its id.
func (c *Client) CreateForumTopic(name string) (int64, error) {
	var res struct {
		MessageThreadID int64 `json:"message_thread_id"`
	}
	err := c.call("createForumTopic", url.Values{
		"chat_id": {strconv.FormatInt(c.chatID, 10)},
		"name":    {name},
	}, &res)
	if err != nil {
		return 0, err
	}
	return res.MessageThreadID, nil
}

// postRetryDelay is a var so tests can shorten it.
var postRetryDelay = 2 * time.Second

// postMessage sends a message with a few retries: a 429 or a network blip
// would otherwise drop the mesh message for good.
func (c *Client) postMessage(params url.Values) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = c.call("sendMessage", params, nil); err == nil {
			return nil
		}
		if attempt < 2 {
			time.Sleep(postRetryDelay)
		}
	}
	return err
}

func (c *Client) GetUpdates(offset int64) ([]Update, error) {
	var updates []Update
	err := c.call("getUpdates", url.Values{
		"offset":          {strconv.FormatInt(offset, 10)},
		"timeout":         {"50"},
		"allowed_updates": {`["message"]`},
	}, &updates)
	return updates, err
}

func (c *Client) call(method string, params url.Values, result any) error {
	resp, err := c.client.PostForm(c.baseURL+"/bot"+c.token+"/"+method, params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var wrap struct {
		OK          bool            `json:"ok"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return fmt.Errorf("%s: decode: %w", method, err)
	}
	if !wrap.OK {
		return fmt.Errorf("%s: %s", method, wrap.Description)
	}
	if result != nil {
		return json.Unmarshal(wrap.Result, result)
	}
	return nil
}
