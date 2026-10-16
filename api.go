package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	mux.HandleFunc("/audit/body-access", s.handleBodyAccessAudit)
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
		s.auditBodyAccess(r, folder, 0, "/mail", r.URL.RawQuery, "bulk_raw_list", 200)
		return
	}
	writeJSON(w, 200, map[string]any{"folder": folder, "count": len(env), "messages": env})
}

func (s *Server) handleMailByUID(w http.ResponseWriter, r *http.Request) {
	// /mail/{uid}?folder=INBOX  (metadata by default)
	// /mail/{uid}/body            (raw RFC822; audited)
	// /mail/{uid}?raw=1|body=1   (raw RFC822; audited)
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 {
		writeJSON(w, 400, map[string]any{"error": "usage: /mail/{uid}[ /body]?folder=INBOX"})
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

	bodyMode := false
	if len(parts) == 3 && parts[2] == "body" {
		bodyMode = true
	}
	if r.URL.Query().Get("raw") == "1" || r.URL.Query().Get("body") == "1" {
		bodyMode = true
	}

	if bodyMode {
		w.Header().Set("Content-Type", "message/rfc822")
		if m.Subject != "" {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%d.eml", m.UID))
		}
		w.Write(m.Raw)
		s.auditBodyAccess(r, folder, uint32(uid), r.URL.Path, r.URL.RawQuery, "body_read", 200)
		return
	}

	// Metadata-only response by default: no raw body. Attachment metadata parsed.
	atts := parseAttachments(m.Raw)
	resp := map[string]any{
		"uid":              m.UID,
		"folder":           m.Folder,
		"message_id":       m.MessageID,
		"from_addr":        m.FromAddr,
		"from_name":        m.FromName,
		"to_addr":          m.To,
		"subject":          m.Subject,
		"date":             m.Date,
		"seen":             m.Seen,
		"stored_at":        m.StoredAt,
		"size":             len(m.Raw),
		"attachment_count": len(atts),
		"attachments":      atts,
	}
	writeJSON(w, 200, resp)
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

func (s *Server) handleBodyAccessAudit(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	entries, err := s.store.ListBodyAccess(limit)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"count": len(entries), "entries": entries})
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	return r.RemoteAddr
}

func (s *Server) auditBodyAccess(r *http.Request, folder string, uid uint32, path, query, outcome string, status int) {
	entry := BodyAccessEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Folder:    folder,
		UID:       uid,
		Method:    r.Method,
		Path:      path,
		Query:     query,
		SourceIP:  clientIP(r),
		UserAgent: r.UserAgent(),
		Reason:    r.Header.Get("X-Audit-Reason"),
		Outcome:   outcome,
		Status:    status,
	}
	log.Printf("AUDIT body-access folder=%s uid=%d path=%s ip=%s ua=%q reason=%q outcome=%s", folder, uid, path, entry.SourceIP, entry.UserAgent, entry.Reason, outcome)
	_ = s.store.LogBodyAccess(entry)
}
