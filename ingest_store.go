package main

import (
	"context"
	"time"
)

// HasMessageID reports whether a message with this Message-ID is already stored.
func (s *Store) HasMessageID(messageID string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE message_id=?`, messageID).Scan(&n)
	return n > 0, err
}

// NextIngestUID returns the next synthetic uid for the INGEST folder.
func (s *Store) NextIngestUID(ctx context.Context) (uint32, error) {
	var u uint32
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(uid),0)+1 FROM messages WHERE folder='INGEST'`).Scan(&u)
	return u, err
}

// InsertIngest stores a pushed (non-IMAP) message under folder="INGEST" with a
// synthetic uid. Separate from Upsert so IMAP sync and ingest never collide.
func (s *Store) InsertIngest(m *Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	uid, err := s.NextIngestUID(ctx)
	if err != nil {
		return err
	}
	m.UID = uid
	_, err = s.db.Exec(`
INSERT INTO messages (uid, folder, message_id, from_addr, from_name, to_addr, subject, date, seen, raw, stored_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		m.UID, m.Folder, m.MessageID, m.FromAddr, m.FromName, m.To, m.Subject,
		m.Date.UTC().Format(time.RFC3339), btoi(m.Seen), m.Raw, m.StoredAt.UTC().Format(time.RFC3339))
	return err
}

// IngestEnvelope is the metadata-only view used for rule matching.
type IngestEnvelope struct {
	ID        uint32
	Folder    string
	MessageID string
	FromAddr  string
	To        string
	Subject   string
	Date      time.Time
}

// ListIngest returns recent INGEST messages (metadata only).
func (s *Store) ListIngest(ctx context.Context, limit int) ([]IngestEnvelope, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT uid, folder, message_id, from_addr, to_addr, subject, date
FROM messages WHERE folder='INGEST' ORDER BY uid DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IngestEnvelope{}
	for rows.Next() {
		var e IngestEnvelope
		var date string
		if err := rows.Scan(&e.ID, &e.Folder, &e.MessageID, &e.FromAddr, &e.To, &e.Subject, &date); err != nil {
			return nil, err
		}
		e.Date, _ = time.Parse(time.RFC3339, date)
		out = append(out, e)
	}
	return out, rows.Err()
}
