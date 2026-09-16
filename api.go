package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	store     *Store
	syncer    *Syncer
	rules     *RuleEngine
	forwarder *Forwarder
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/mail", s.handleMail)
	mux.HandleFunc("/mail/", s.handleMailByUID)
	mux.HandleFunc("/sync", s.handleSync)
	mux.HandleFunc("/ingest", s.handleIngest)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	n, err := s.store.Count("INBOX")
	if err != nil {
		n = -1
	}
	u, _ := s.store.UnseenCount("INBOX")
	writeJSON(w, 200, map[string]any{"ok": true, "inbox": n, "unseen": u, "time": time.Now().UTC().Format(time.RFC3339)})
}

func (s *Server) handleMail(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	folder := q.Get("folder")
	if folder == "" {
		folder = "INBOX"
	}
	limit := 50
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	unseen := q.Get("unseen") == "1" || q.Get("unseen") == "true"

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var (
		env []Envelope
		err error
	)
	if sq := strings.TrimSpace(q.Get("search")); sq != "" {
		env, err = s.store.Search(ctx, folder, sq, limit)
	} else {
		env, err = s.store.List(ctx, folder, limit, unseen)
	}
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	// Accept ?raw=1 to return raw bodies inline (pairs with ?search).
	if q.Get("raw") == "1" {
		out := make([]map[string]any, 0, len(env))
		for _, e := range env {
			m, _ := s.store.Get(ctx, e.Folder, e.UID)
			row := map[string]any{"uid": e.UID, "folder": e.Folder, "from": e.From, "subject": e.Subject, "date": e.Date, "seen": e.Seen}
			if m != nil {
				row["raw"] = string(m.Raw)
			}
			out = append(out, row)
		}
		writeJSON(w, 200, out)
		return
	}
	writeJSON(w, 200, map[string]any{"folder": folder, "count": len(env), "messages": env})
}

func (s *Server) handleMailByUID(w http.ResponseWriter, r *http.Request) {
	// /mail/{uid}?folder=INBOX
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 2 {
		writeJSON(w, 400, map[string]any{"error": "usage: /mail/{uid}?folder=INBOX"})
		return
	}
	uid, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "uid must be an integer"})
		return
	}
	folder := r.URL.Query().Get("folder")
	if folder == "" {
		folder = "INBOX"
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	m, err := s.store.Get(ctx, folder, uint32(uid))
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	if m == nil {
		writeJSON(w, 404, map[string]any{"error": "not found"})
		return
	}
	if r.URL.Query().Get("raw") == "1" {
		w.Header().Set("Content-Type", "message/rfc822")
		if m.Subject != "" {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%d.eml", m.UID))
		}
		w.Write(m.Raw)
		return
	}
	writeJSON(w, 200, m)
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]any{"error": "POST required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	if err := s.syncer.Sync(ctx, "INBOX"); err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	n, _ := s.store.Count("INBOX")
	u, _ := s.store.UnseenCount("INBOX")
	writeJSON(w, 200, map[string]any{"ok": true, "inbox": n, "unseen": u})
}
