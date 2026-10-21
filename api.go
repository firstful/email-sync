package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
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
	n, err := s.store.Count("INGEST")
	if err != nil {
		n = -1
	}
	u, _ := s.store.UnseenCount("INGEST")
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
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			// Clamp instead of silently falling back to the default: a caller
			// asking for 1000 used to get 50 (same failure class as a missing
			// field — silently less than requested).
			if n > 500 {
				n = 500
			}
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
		s.writeBulkRaw(w, r, env)
		s.auditBodyAccess(r, folder, 0, "/mail", r.URL.RawQuery, "bulk_raw_list", 200)
		return
	}
	writeJSON(w, 200, map[string]any{"folder": folder, "count": len(env), "messages": env})
}

// defaultMaxRawBytes caps the raw payload served by a single bulk ?raw=1
// request (64 MiB). Override with MAX_RAW_BYTES. The cap exists so a growing
// store can never produce a multi-hundred-MB response — the previous
// implementation buffered every body AND then marshalled the whole slice
// into a second buffer, which OOMKilled the 256Mi pod (exit 137).
const defaultMaxRawBytes = 64 * 1024 * 1024

func maxRawBytes() int64 {
	v := os.Getenv("MAX_RAW_BYTES")
	if v == "" {
		return defaultMaxRawBytes
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		log.Printf("WARN: invalid MAX_RAW_BYTES=%q, using default %d", v, defaultMaxRawBytes)
		return defaultMaxRawBytes
	}
	return n
}

// writeBulkRaw streams the bulk ?raw=1 response: a bare JSON array whose
// elements carry uid, folder, from, subject, date, seen, raw. Each row is
// encoded and written one at a time, so only ONE message body is resident in
// memory at any moment. If the raw byte cap is reached the array is closed
// cleanly (valid JSON, fewer elements) — never truncated, never a 500.
func (s *Server) writeBulkRaw(w http.ResponseWriter, r *http.Request, env []Envelope) {
	limit := maxRawBytes()

	// The envelope list used a 10s timeout; streaming a large payload needs a
	// longer budget for the per-message reads.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(200)

	var (
		served   int64
		included int
		skipped  int
		capped   bool
	)

	_, _ = w.Write([]byte("["))
	for _, e := range env {
		if included > 0 && served >= limit {
			capped = true
			skipped = len(env) - included
			break
		}
		m, err := s.store.Get(ctx, e.Folder, e.UID)
		if err != nil {
			log.Printf("bulk raw: get folder=%s uid=%d: %v", e.Folder, e.UID, err)
			break
		}
		raw := ""
		rawLen := 0
		if m != nil {
			raw = string(m.Raw)
			rawLen = len(m.Raw)
			if included > 0 && served+int64(rawLen) > limit {
				capped = true
				skipped = len(env) - included
				break
			}
		}
		row := map[string]any{
			"uid":     e.UID,
			"folder":  e.Folder,
			"from":    e.From,
			"subject": e.Subject,
			"date":    e.Date,
			"seen":    e.Seen,
			"raw":     raw,
		}
		buf, err := json.Marshal(row)
		if err != nil {
			log.Printf("bulk raw: marshal uid=%d: %v", e.UID, err)
			break
		}
		if included > 0 {
			if _, err := w.Write([]byte(",")); err != nil {
				break
			}
		}
		if _, err := w.Write(buf); err != nil {
			break
		}
		served += int64(rawLen)
		included++
	}
	_, _ = w.Write([]byte("]"))
	_, _ = w.Write([]byte("\n"))
	if capped {
		log.Printf("bulk raw: MAX_RAW_BYTES cap %d reached — included %d message(s), skipped %d, served %d raw bytes",
			limit, included, skipped, served)
	} else {
		log.Printf("bulk raw: included %d message(s), served %d raw bytes (cap %d)",
			included, served, limit)
	}
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
