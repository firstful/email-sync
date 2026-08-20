package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"
)

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	port := getenv("PORT", "8080")
	dbPath := getenv("DB_PATH", "/data/email.db")

	store, err := NewStore(dbPath)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer store.Close()

	host := os.Getenv("IMAP_HOST")
	pass := os.Getenv("IMAP_PASSWORD")
	if host == "" || pass == "" {
		log.Printf("WARN: IMAP_HOST/IMAP_PASSWORD not set — running idle (no sync) until the Secret is populated")
	}

	interval, _ := time.ParseDuration(getenv("SYNC_INTERVAL", "60s"))
	if interval <= 0 {
		interval = time.Minute
	}
	maxFetch := atoui(getenv("MAX_FETCH", "500"))
	if maxFetch == 0 {
		maxFetch = 500
	}

	syncer := NewSyncer(store, host, getenv("IMAP_PORT", "993"),
		os.Getenv("IMAP_USERNAME"), pass, interval, maxFetch)
	srv := &Server{store: store, syncer: syncer}

	// Background sync loop.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go syncer.Run(ctx)

	s := &http.Server{
		Addr:              ":" + port,
		Handler:           srv.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("email-sync listening on :%s (db=%s)", port, dbPath)
	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}
