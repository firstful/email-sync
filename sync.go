package main

import (
	"context"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	imap "github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
)

type Syncer struct {
	store    *Store
	host     string
	port     string
	username string
	password string
	interval time.Duration
	maxFetch uint32
	// inFlight guard: only one sync at a time (sync endpoint + ticker).
	busy chan struct{}
}

func NewSyncer(st *Store, host, port, user, pass string, interval time.Duration, maxFetch uint32) *Syncer {
	return &Syncer{
		store: st, host: host, port: port, username: user, password: pass,
		interval: interval, maxFetch: maxFetch, busy: make(chan struct{}, 1),
	}
}

// Run blocks, syncing immediately then on the ticker.
func (s *Syncer) Run(ctx context.Context) {
	if err := s.Sync(ctx, "INBOX"); err != nil {
		log.Printf("initial sync failed: %v", err)
	}
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.Sync(ctx, "INBOX"); err != nil {
				log.Printf("sync failed: %v", err)
			}
		}
	}
}

// Sync pulls new messages (by UID greater than the last stored) from folder.
// Returns immediately if a sync is already in flight.
func (s *Syncer) Sync(ctx context.Context, folder string) error {
	select {
	case s.busy <- struct{}{}:
		defer func() { <-s.busy }()
	default:
		return nil // already syncing
	}

	addr := net.JoinHostPort(s.host, s.port)
	c, err := client.DialTLS(addr, nil)
	if err != nil {
		return err
	}
	defer c.Logout()

	if err := c.Login(s.username, s.password); err != nil {
		return err
	}

	mbox, err := c.Select(folder, true)
	if err != nil {
		return err
	}

	last, err := s.store.LastUID(ctx, folder)
	if err != nil {
		return err
	}

	// Nothing new.
	if mbox.Messages == 0 || mbox.UidNext == 0 || last >= mbox.UidNext {
		return nil
	}

	// Search for UIDs strictly greater than the last stored one.
	crit := imap.NewSearchCriteria()
	crit.Uid = &imap.SeqSet{}
	if last > 0 {
		crit.Uid.AddRange(last+1, 0) // last+1 .. *
	} else {
		crit.Uid.AddRange(1, 0)
	}
	uids, err := c.UidSearch(crit)
	if err != nil {
		return err
	}
	if len(uids) == 0 {
		return nil
	}

	// Cap a first-run backfill so we don't pull the whole mailbox at once.
	ids := uids
	if len(ids) > int(s.maxFetch) {
		ids = ids[len(ids)-int(s.maxFetch):]
	}

	seq := new(imap.SeqSet)
	for _, u := range ids {
		seq.AddNum(u)
	}

	msgs := make(chan *imap.Message, 20)
	done := make(chan error, 1)
	go func() {
		done <- c.UidFetch(seq, []imap.FetchItem{imap.FetchEnvelope, imap.FetchRFC822, imap.FetchFlags}, msgs)
	}()

	n := 0
	for msg := range msgs {
		m, err := convert(msg, folder)
		if err != nil {
			log.Printf("convert uid=%d: %v", msg.Uid, err)
			continue
		}
		if err := s.store.Upsert(m); err != nil {
			log.Printf("store uid=%d: %v", msg.Uid, err)
			continue
		}
		n++
	}
	if err := <-done; err != nil {
		return err
	}
	log.Printf("synced %d new message(s) in %s (now %d total)", n, folder, mustCount(s.store, folder))
	return nil
}

func convert(msg *imap.Message, folder string) (*Message, error) {
	m := &Message{
		UID:     msg.Uid,
		Folder:  folder,
		StoredAt: time.Now().UTC(),
	}
	if msg.Envelope != nil {
		e := msg.Envelope
		m.MessageID = e.MessageId
		m.Subject = e.Subject
		if len(e.From) > 0 {
			m.FromName = e.From[0].PersonalName
			m.FromAddr = e.From[0].Address()
		}
		if len(e.To) > 0 {
			m.To = e.To[0].Address()
		}
		if !e.Date.IsZero() {
			m.Date = e.Date
		}
	}
	for _, fl := range msg.Flags {
		if string(fl) == `\Seen` {
			m.Seen = true
		}
	}
	// Body arrives as a map[*BodySectionName]Literal; pull the whole message.
	if lit := msg.GetBody(&imap.BodySectionName{}); lit != nil {
		raw, err := io.ReadAll(lit)
		if err != nil {
			return nil, err
		}
		m.Raw = raw
	}
	return m, nil
}

func mustCount(st *Store, folder string) int {
	n, err := st.Count(folder)
	if err != nil {
		return -1
	}
	return n
}

func atoui(s string) uint32 {
	v, _ := strconv.ParseUint(s, 10, 32)
	return uint32(v)
}
