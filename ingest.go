package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"io"
	"mime"
	"mime/quotedprintable"
	"net/http"
	"net/mail"
	"os"
	"strings"
	"time"
)

// IngestMaxBytes caps raw MIME size (Email Routing Worker payloads).
const IngestMaxBytes = 4 << 20 // 4 MiB — CF Email Routing itself caps lower

// handleIngest accepts a raw RFC822 message pushed by the CF Email Routing
// Worker. Auth: X-Ingest-Token must equal INGEST_TOKEN env (constant-time).
// Dedup: X-Message-ID (or parsed Message-ID) — duplicate POSTs are no-ops.
// Storage: folder="INGEST", uid = monotonically increasing synthetic id.
// After store, the rule engine is notified (metadata only — never raw content).
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]any{"error": "POST required"})
		return
	}
	if tok := os.Getenv("INGEST_TOKEN"); tok != "" {
		got := r.Header.Get("X-Ingest-Token")
		h1 := sha256.Sum256([]byte(got))
		h2 := sha256.Sum256([]byte(tok))
		if subtle.ConstantTimeCompare(h1[:], h2[:]) != 1 {
			writeJSON(w, 401, map[string]any{"error": "bad ingest token"})
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, IngestMaxBytes+1))
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "read failed"})
		return
	}
	if len(body) == 0 {
		writeJSON(w, 400, map[string]any{"error": "empty body"})
		return
	}
	if len(body) > IngestMaxBytes {
		writeJSON(w, 413, map[string]any{"error": "message too large"})
		return
	}

	msg, err := mail.ReadMessage(strings.NewReader(string(body)))
	if err != nil {
		writeJSON(w, 422, map[string]any{"error": "not parseable RFC822: " + err.Error()})
		return
	}

	m := &Message{Folder: "INGEST", StoredAt: time.Now().UTC()}
	m.MessageID = r.Header.Get("X-Message-ID")
	if m.MessageID == "" {
		m.MessageID = msg.Header.Get("Message-ID")
	}
	if addr, err := mail.ParseAddress(msg.Header.Get("From")); err == nil {
		m.FromAddr = addr.Address
		m.FromName = addr.Name
	}
	if addr, err := mail.ParseAddress(msg.Header.Get("To")); err == nil {
		m.To = addr.Address
	}
	m.Subject = decodeHeader(msg.Header.Get("Subject"))
	m.Raw = body
	if d, err := mail.ParseDate(msg.Header.Get("Date")); err == nil {
		m.Date = d
	} else {
		m.Date = time.Now().UTC()
	}
	// Ingested mail is unseen by definition.
	m.Seen = false

	// Dedup by message_id when present.
	if m.MessageID != "" {
		dup, err := s.store.HasMessageID(m.MessageID)
		if err == nil && dup {
			writeJSON(w, 200, map[string]any{"ok": true, "duplicate": true})
			return
		}
	}

	if err := s.store.InsertIngest(m); err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}

	// Rule engine: metadata-only dispatch (never raw content in payloads).
	if s.rules != nil {
		s.rules.Dispatch(m)
	}
	// Best-effort SMTP forward for whitelisted recipients.
	s.forwarder.MaybeForward(m)
	writeJSON(w, 200, map[string]any{"ok": true, "id": m.UID, "folder": m.Folder})
}

// sanitizeHeader strips CR/LF and truncates — safe for logging/echo/payloads.
func sanitizeHeader(s string) string {
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.ReplaceAll(s, "\n", "")
	if len(s) > 256 {
		s = s[:256]
	}
	return s
}

// decodeHeader decodes RFC2047 words if present.
func decodeHeader(s string) string {
	d := mime.WordDecoder{CharsetReader: func(charset string, input io.Reader) (io.Reader, error) {
		switch strings.ToLower(charset) {
		case "utf-8", "us-ascii", "iso-8859-1":
			return input, nil
		case "quoted-printable":
			return quotedprintable.NewReader(input), nil
		}
		return nil, fmt.Errorf("unsupported charset %q", charset)
	}}
	if out, err := d.DecodeHeader(s); err == nil {
		return out
	}
	return s
}
