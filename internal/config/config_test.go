package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.env")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONFIG_FILE", path)
	// keep the developer's shell out of the test
	t.Setenv("BOT_TOKEN", "")
	t.Setenv("TARGET_CHAT_ID", "")
	t.Setenv("RADIO_ADDR", "")
	t.Setenv("CHANNEL_NAME", "")
}

func TestLoadDefaultsAndEnv(t *testing.T) {
	writeConfig(t, "")

	t.Setenv("BOT_TOKEN", "TOK")
	t.Setenv("TARGET_CHAT_ID", "-100500")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RadioAddr != "127.0.0.1:5000" || c.ChannelName != "Public" || c.TopicsFile != "topics.json" {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if c.TzOffset != 5 {
		t.Fatalf("default TZ_OFFSET = %v, want +5", c.TzOffset)
	}
	if c.ChatID != -100500 || c.BotToken != "TOK" {
		t.Fatalf("env wrong: %+v", c)
	}

	t.Setenv("BOT_TOKEN", "")
	if _, err := Load(); err == nil {
		t.Fatal("missing BOT_TOKEN must fail")
	}

	t.Setenv("BOT_TOKEN", "TOK")
	t.Setenv("TARGET_CHAT_ID", "@channelname")
	if _, err := Load(); err == nil {
		t.Fatal("non-numeric chat id must fail")
	}
}

func TestLoadFromFile(t *testing.T) {
	writeConfig(t, "# комментарий\n"+
		"BOT_TOKEN=FILETOK\n"+
		"TARGET_CHAT_ID=-100777\n"+
		"RADIO_ADDR=10.0.0.5:5000\n"+
		"CHANNEL_NAME=Ops\n")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.BotToken != "FILETOK" || c.ChatID != -100777 {
		t.Fatalf("file values wrong: %+v", c)
	}
	if c.RadioAddr != "10.0.0.5:5000" || c.ChannelName != "Ops" {
		t.Fatalf("file values wrong: %+v", c)
	}

	// переменные окружения переопределяют файл
	t.Setenv("BOT_TOKEN", "ENVTOK")
	c2, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c2.BotToken != "ENVTOK" {
		t.Fatalf("env must override file, got %q", c2.BotToken)
	}
}

func TestLoadFileMissing(t *testing.T) {
	t.Setenv("CONFIG_FILE", filepath.Join(t.TempDir(), "nope.env"))
	if _, err := Load(); err == nil {
		t.Fatal("explicit CONFIG_FILE that does not exist must fail")
	}

	// default .env is optional: env-only mode must still work
	t.Setenv("CONFIG_FILE", "")
	t.Setenv("BOT_TOKEN", "TOK")
	t.Setenv("TARGET_CHAT_ID", "-100500")
	t.Chdir(t.TempDir()) // no .env here
	if _, err := Load(); err != nil {
		t.Fatalf("env-only mode must work: %v", err)
	}
}

func TestLoadBadLine(t *testing.T) {
	writeConfig(t, "GARBAGE WITHOUT EQUALS\n")
	if _, err := Load(); err == nil {
		t.Fatal("malformed line must fail")
	}
}

func TestLoadStripsQuotes(t *testing.T) {
	writeConfig(t, "BOT_TOKEN=\"TOK\"\nTARGET_CHAT_ID='-100777'\n")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.BotToken != "TOK" || c.ChatID != -100777 {
		t.Fatalf("quotes not stripped: %+v", c)
	}
}

func TestLoadErrorListsFoundKeys(t *testing.T) {
	writeConfig(t, "BOT_TOKEN=TOK\nRADIO_ADDR=10.0.0.5:5000\n")

	_, err := Load()
	if err == nil {
		t.Fatal("missing TARGET_CHAT_ID must fail")
	}
	msg := err.Error()
	for _, want := range []string{"TARGET_CHAT_ID", "BOT_TOKEN", "RADIO_ADDR"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error must list keys, got %q", msg)
		}
	}
}

func TestLoadTzOffset(t *testing.T) {
	writeConfig(t, "BOT_TOKEN=TOK\nTARGET_CHAT_ID=-100\nTZ_OFFSET=5.5\n")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.TzOffset != 5.5 {
		t.Fatalf("TZ_OFFSET = %v, want 5.5", c.TzOffset)
	}

	writeConfig(t, "BOT_TOKEN=TOK\nTARGET_CHAT_ID=-100\nTZ_OFFSET=abc\n")
	if _, err := Load(); err == nil {
		t.Fatal("bad TZ_OFFSET must fail")
	}
}
