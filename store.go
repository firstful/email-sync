package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Message is a synced email envelope + raw payload reference.
type Message struct {
	UID       uint32
	Folder    string
	MessageID string
	FromAddr  string
	FromName  string
	To        string
	Subject   string
	Date      time.Time
	Seen      bool
	Raw       []byte
	StoredAt  time.Time
}

type Store struct {
	db *sql.DB
}

func NewStore(path string) (*Store, error) {
	// busy_timeout + WAL so the sync goroutine and HTTP readers don't contend.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // modernc/sqlite is single-writer; serialise access
	st := &Store{db: db}
	if err := st.migrate(); err != nil {
		return nil, err
	}
	return st, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS messages (
	uid       INTEGER NOT NULL,
	folder    TEXT    NOT NULL,
	message_id TEXT   NOT NULL DEFAULT '',
	from_addr TEXT    NOT NULL DEFAULT '',
	from_name TEXT    NOT NULL DEFAULT '',
	to_addr   TEXT    NOT NULL DEFAULT '',
	subject   TEXT    NOT NULL DEFAULT '',
	date      TEXT    NOT NULL DEFAULT '',
	seen      INTEGER NOT NULL DEFAULT 0,
	raw       BLOB    NOT NULL,
	stored_at TEXT    NOT NULL DEFAULT '',
	PRIMARY KEY (folder, uid)
);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC);
`
	_, err := s.db.Exec(schema)
	return err
}

func (s *Store) Upsert(m *Message) error {
	_, err := s.db.Exec(`
INSERT INTO messages (uid, folder, message_id, from_addr, from_name, to_addr, subject, date, seen, raw, stored_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(folder, uid) DO UPDATE SET
  seen=excluded.seen, subject=excluded.subject, raw=excluded.raw`,
		m.UID, m.Folder, m.MessageID, m.FromAddr, m.FromName, m.To, m.Subject,
		m.Date.UTC().Format(time.RFC3339), btoi(m.Seen), m.Raw, time.Now().UTC().Format(time.RFC3339))
	return err
}

func (s *Store) LastUID(ctx context.Context, folder string) (uint32, error) {
	var u uint32
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(uid),0) FROM messages WHERE folder=?`, folder).Scan(&u)
	return u, err
}

func (s *Store) Count(folder string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE folder=?`, folder).Scan(&n)
	return n, err
}

func (s *Store) UnseenCount(folder string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE folder=? AND seen=0`, folder).Scan(&n)
	return n, err
}

type Envelope struct {
	UID     uint32 `json:"uid"`
	Folder  string `json:"folder"`
	From    string `json:"from"`
	Subject string `json:"subject"`
	Date    string `json:"date"`
	Seen    bool   `json:"seen"`
	Size    int    `json:"size"`
}

func rowToEnvelope(rows *sql.Rows) (Envelope, error) {
	var e Envelope
	var rawLen int
	var date string
	if err := rows.Scan(&e.UID, &e.Folder, &e.From, &e.Subject, &date, &e.Seen, &rawLen); err != nil {
		return e, err
	}
	e.Date = date
	e.Size = rawLen
	return e, nil
}

func (s *Store) List(ctx context.Context, folder string, limit int, unseenOnly bool) ([]Envelope, error) {
	q := `SELECT uid, folder, from_addr, subject, date, seen, LENGTH(raw) FROM messages WHERE 1=1`
	args := []any{}
	if folder != "" {
		q += ` AND folder=?`
		args = append(args, folder)
	}
	if unseenOnly {
		q += ` AND seen=0`
	}
	q += ` ORDER BY date DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Envelope{}
	for rows.Next() {
		e, err := rowToEnvelope(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Get(ctx context.Context, folder string, uid uint32) (*Message, error) {
	m := &Message{}
	var date string
	err := s.db.QueryRowContext(ctx,
		`SELECT uid, folder, message_id, from_addr, from_name, to_addr, subject, date, seen, raw FROM messages WHERE folder=? AND uid=?`,
		folder, uid).Scan(&m.UID, &m.Folder, &m.MessageID, &m.FromAddr, &m.FromName, &m.To, &m.Subject, &date, &m.Seen, &m.Raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m.Date, _ = time.Parse(time.RFC3339, date)
	return m, nil
}

// Search does a naive case-insensitive LIKE across subject/from/raw. Good
// enough for a first cut; swap for FTS5 if we need proper full-text.
func (s *Store) Search(ctx context.Context, folder, query string, limit int) ([]Envelope, error) {
	q := `SELECT uid, folder, from_addr, subject, date, seen, LENGTH(raw) FROM messages WHERE 1=1`
	args := []any{}
	if folder != "" {
		q += ` AND folder=?`
		args = append(args, folder)
	}
	like := "%" + strings.ToLower(query) + "%"
	q += ` AND (lower(subject) LIKE ? OR lower(from_addr) LIKE ? OR lower(raw) LIKE ?) ORDER BY date DESC LIMIT ?`
	args = append(args, like, like, like, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Envelope{}
	for rows.Next() {
		e, err := rowToEnvelope(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
