package main

import (
	"log"
	"time"

	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/bridge"
	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/companion"
	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/config"
	"github.com/shadowroot2/MeshCore-Telegram-Bot/internal/telegram"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	tg := telegram.New(cfg.BotToken, cfg.ChatID)
	b := bridge.New(tg, cfg.TopicsFile, cfg.TzOffset)
	log.Printf("bridge: %s/%s <-> telegram %d (topics: %s)",
		cfg.RadioAddr, cfg.ChannelName, cfg.ChatID, cfg.TopicsFile)

	go bridge.PollTelegram(b)

	for {
		err := companion.Session(cfg.RadioAddr, cfg.ChannelName, b, b.Deliver)
		log.Printf("session: %v, reconnecting", err)
		time.Sleep(3 * time.Second)
	}
}
