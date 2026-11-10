package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type telegram struct {
	token  string
	chatID int64
	client *http.Client
}

func newTelegram(token string, chatID int64) *telegram {
	return &telegram{
		token:  token,
		chatID: chatID,
		client: &http.Client{Timeout: 65 * time.Second}, // > getUpdates long-poll
	}
}

type tgUpdate struct {
	UpdateID int64      `json:"update_id"`
	Message  *tgMessage `json:"message"`
}

type tgMessage struct {
	MessageID int64  `json:"message_id"`
	Text      string `json:"text"`
	Chat      struct {
		ID int64 `json:"id"`
	} `json:"chat"`
	From struct {
		ID        int64  `json:"id"`
		IsBot     bool   `json:"is_bot"`
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Username  string `json:"username"`
	} `json:"from"`
}

func (t *telegram) sendMessage(text string) error {
	return t.postMessage(url.Values{
		"chat_id": {strconv.FormatInt(t.chatID, 10)},
		"text":    {text},
	})
}

func (t *telegram) replyToMessage(replyToMsgID int64, text string) error {
	return t.postMessage(url.Values{
		"chat_id":             {strconv.FormatInt(t.chatID, 10)},
		"reply_to_message_id": {strconv.FormatInt(replyToMsgID, 10)},
		"text":                {text},
	})
}

// postMessage sends a message with a few retries: a 429 or a network blip
// would otherwise drop the mesh message for good.
func (t *telegram) postMessage(params url.Values) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = t.call("sendMessage", params, nil); err == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return err
}

func (t *telegram) getUpdates(offset int64) ([]tgUpdate, error) {
	var updates []tgUpdate
	err := t.call("getUpdates", url.Values{
		"offset":          {strconv.FormatInt(offset, 10)},
		"timeout":         {"50"},
		"allowed_updates": {`["message"]`},
	}, &updates)
	return updates, err
}

func (t *telegram) call(method string, params url.Values, result any) error {
	resp, err := t.client.PostForm("https://api.telegram.org/bot"+t.token+"/"+method, params)
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

func parseChatID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errors.New("TARGET_CHAT_ID must be numeric")
	}
	return id, nil
}
