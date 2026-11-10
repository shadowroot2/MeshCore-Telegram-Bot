package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	RadioAddr   string
	ChannelName string
	BotToken    string
	ChatID      int64
	TopicsFile  string
}

// Load resolves settings with the precedence: defaults < config file <
// environment variables. The file is CONFIG_FILE, default .env; the default
// file is optional, an explicitly named one must exist.
func Load() (Config, error) {
	var c Config
	c.RadioAddr = "127.0.0.1:5000"
	c.ChannelName = "Public"
	c.TopicsFile = "topics.json"

	path := os.Getenv("CONFIG_FILE")
	explicit := path != ""
	if !explicit {
		path = ".env"
	}
	vals, err := readConfigFile(path)
	if err != nil {
		if explicit || !errors.Is(err, os.ErrNotExist) {
			return c, err
		}
		vals = map[string]string{}
	}

	get := func(key string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return vals[key]
	}

	c.BotToken = get("BOT_TOKEN")
	if c.BotToken == "" {
		return c, errors.New("BOT_TOKEN is empty (.env or environment)")
	}
	if v := get("RADIO_ADDR"); v != "" {
		c.RadioAddr = v
	}
	if v := get("CHANNEL_NAME"); v != "" {
		c.ChannelName = v
	}
	if v := get("TOPICS_FILE"); v != "" {
		c.TopicsFile = v
	}
	target := get("TARGET_CHAT_ID")
	if target == "" {
		return c, errors.New("TARGET_CHAT_ID is empty (.env or environment)")
	}
	chatID, err := strconv.ParseInt(target, 10, 64)
	if err != nil {
		return c, errors.New("TARGET_CHAT_ID must be numeric")
	}
	c.ChatID = chatID
	return c, nil
}

// readConfigFile parses a KEY=VALUE file: blank lines and `#` comments are
// ignored, the value is everything after the first `=`.
func readConfigFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	vals := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s: not KEY=VALUE: %q", path, line)
		}
		vals[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return vals, nil
}
