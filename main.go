package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	radioAddr   string
	channelName string
	botToken    string
	chatID      int64
}

func loadConfig() (config, error) {
	var c config
	c.radioAddr = os.Getenv("RADIO_ADDR")
	if c.radioAddr == "" {
		c.radioAddr = "127.0.0.1:5000"
	}
	c.channelName = os.Getenv("CHANNEL_NAME")
	if c.channelName == "" {
		c.channelName = "Public"
	}
	c.botToken = os.Getenv("BOT_TOKEN")
	if c.botToken == "" {
		return c, errors.New("BOT_TOKEN is empty")
	}
	var err error
	if c.chatID, err = parseChatID(os.Getenv("TARGET_CHAT_ID")); err != nil {
		return c, err
	}
	return c, nil
}

// bridge holds the live companion session so the Telegram poller can push
// texts into the mesh while the session loop drains messages out of it.
type bridge struct {
	mu  sync.Mutex
	c   *companion
	idx byte
}

func (b *bridge) attach(c *companion, idx byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.c, b.idx = c, idx
}

func (b *bridge) detach() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.c = nil
}

func (b *bridge) online() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.c != nil
}

func (b *bridge) sendText(text string) (uint32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.c == nil {
		return 0, errors.New("radio offline")
	}
	return b.c.sendChannelText(b.idx, text)
}

// heardCount asks the radio how many neighbours repeated the flood for the
// send with that message timestamp (CLI `send.heard <ts>`). An unparseable
// reply (e.g. a firmware without `send.heard`) is reported as -1.
func (b *bridge) heardCount(ts uint32) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.c == nil {
		return 0, errors.New("radio offline")
	}
	reply, err := b.c.runCLI(fmt.Sprintf("send.heard %d", ts))
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

// formatForTelegram renders the mesh metadata header the way the mock showed:
// "18:37 · SNR 8.5 dB" on its own line above the text. A zero mesh timestamp
// (no clock on the sending node) is left out rather than shown as epoch.
func formatForTelegram(m meshMsg) string {
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

const maxNameLen = 32 // sender name budget before it eats the mesh text

// meshText is what goes into the mesh for a Telegram message: "<name>: <text>".
// Whitespace-only messages yield "" and are skipped; a very long display name
// is cut so the message body still fits the frame.
func meshText(m *tgMessage) string {
	body := strings.TrimSpace(m.Text)
	if body == "" {
		return ""
	}
	name := truncate(senderName(m), maxNameLen)
	return name + ": " + body
}

func senderName(m *tgMessage) string {
	name := strings.TrimSpace(m.From.FirstName + " " + m.From.LastName)
	if name == "" && m.From.Username != "" {
		name = "@" + m.From.Username
	}
	if name == "" {
		name = fmt.Sprintf("id %d", m.From.ID)
	}
	return name
}

// heardCheckDelay matches the firmware's CHANNEL_FEEDBACK_WINDOW_MS: that
// long after the send the retransmit dupes are worth extracting.
const heardCheckDelay = 5 * time.Second

// checkHeard replies to the user's message with how many repeaters repeated
// the flood. Stays silent when the radio is unreachable at check time.
func checkHeard(tg *telegram, b *bridge, replyTo int64, ts uint32) {
	time.Sleep(heardCheckDelay)
	n, err := b.heardCount(ts)
	if err != nil {
		log.Printf("heard check: %v", err)
		return
	}
	if err := tg.replyToMessage(replyTo, formatHeard(n)); err != nil {
		log.Printf("telegram: %v", err)
	}
}

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

// pollTelegram: long-poll the group and push each text into the mesh. The
// Telegram offset advances only after the radio acked, so a down radio
// redelivers instead of dropping messages.
func pollTelegram(tg *telegram, b *bridge) {
	var offset int64
	for {
		updates, err := tg.getUpdates(offset)
		if err != nil {
			log.Printf("telegram: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		for _, u := range updates {
			m := u.Message
			if m == nil || m.Chat.ID != tg.chatID || m.From.IsBot {
				offset = u.UpdateID + 1
				continue
			}
			text := meshText(m)
			if text == "" {
				offset = u.UpdateID + 1
				continue
			}
			ts, err := b.sendText(text)
			if err != nil {
				log.Printf("mesh: %v, will retry %q", err, text)
				time.Sleep(3 * time.Second)
				break
			}
			offset = u.UpdateID + 1
			go checkHeard(tg, b, m.MessageID, ts)
		}
	}
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	tg := newTelegram(cfg.botToken, cfg.chatID)
	b := &bridge{}
	log.Printf("bridge: %s/%s <-> telegram %d", cfg.radioAddr, cfg.channelName, cfg.chatID)

	go pollTelegram(tg, b)

	for {
		err := session(cfg.radioAddr, cfg.channelName, b, func(m meshMsg) error {
			return tg.sendMessage(formatForTelegram(m))
		})
		log.Printf("session: %v, reconnecting", err)
		time.Sleep(3 * time.Second)
	}
}
