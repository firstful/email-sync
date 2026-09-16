package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// SMTP forwarder (Reed 09-16, optional): re-send ingested mail whose recipient
// matches ForwardMatch to a smarthost (Namecheap Private Email). Best-effort:
// failures are logged, never retried into a loop, never fail the ingest.
// Config env:
//   SMTP_FORWARD_MATCH  — anchored regex on the envelope recipient (required to enable)
//   SMTP_FORWARD_HOST   — smarthost hostname (e.g. mx1.privateemail.com)
//   SMTP_FORWARD_PORT   — default 25
//   SMTP_FORWARD_FROM   — envelope-from override (default: original recipient,
//                         so Namecheap's SPF-of-ffilt.us alignment isn't needed —
//                         we deliver AS the recipient address to its own mailbox)
type Forwarder struct {
	match *regexp.Regexp
	host  string
	port  string
	from  string

	// dedupe: message-ids attempted this process lifetime
	mu    sync.Mutex
	tried map[string]struct{}
}

func NewForwarder() *Forwarder {
	pat := os.Getenv("SMTP_FORWARD_MATCH")
	host := os.Getenv("SMTP_FORWARD_HOST")
	if pat == "" || host == "" {
		return nil
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		log.Printf("forwarder: bad SMTP_FORWARD_MATCH regex, disabled: %v", err)
		return nil
	}
	f := &Forwarder{
		match: re,
		host:  host,
		port:  os.Getenv("SMTP_FORWARD_PORT"),
		from:  os.Getenv("SMTP_FORWARD_FROM"),
		tried: map[string]struct{}{},
	}
	if f.port == "" {
		f.port = "25"
	}
	log.Printf("forwarder: enabled → %s:%s match=%q", f.host, f.port, pat)
	return f
}

// MaybeForward delivers m to the smarthost if its recipient matches. Non-blocking.
func (f *Forwarder) MaybeForward(m *Message) {
	if f == nil || m == nil || m.To == "" {
		return
	}
	if !f.match.MatchString(strings.ToLower(m.To)) {
		return
	}
	// Dedup: a re-POST of the same message must not double-deliver.
	if m.MessageID != "" {
		f.mu.Lock()
		if _, ok := f.tried[m.MessageID]; ok {
			f.mu.Unlock()
			return
		}
		f.tried[m.MessageID] = struct{}{}
		f.mu.Unlock()
	}
	go func() {
		if err := f.deliver(m); err != nil {
			log.Printf("forwarder: deliver %q → %s failed: %v", sanitizeHeader(m.Subject), m.To, err)
		} else {
			log.Printf("forwarder: delivered uid=%d to %s", m.UID, m.To)
		}
	}()
}

// deliver speaks plain SMTP to the smarthost. The recipient's mailbox lives on
// the smarthost, so delivery is RCPT TO the mailbox itself — no relay auth
// needed (it's the destination MX for that mailbox). Envelope-from is the
// recipient address (mailbox-internal delivery) unless overridden.
func (f *Forwarder) deliver(m *Message) error {
	from := f.from
	if from == "" {
		from = m.To
	}
	hostPort := net.JoinHostPort(f.host, f.port)

	var conn net.Conn
	var err error
	if f.port == "465" {
		d := &net.Dialer{Timeout: 30 * time.Second}
		conn, err = tls.DialWithDialer(d, "tcp", hostPort, &tls.Config{ServerName: f.host})
	} else {
		conn, err = net.DialTimeout("tcp", hostPort, 30*time.Second)
	}
	if err != nil {
		return err
	}
	c, err := smtp.NewClient(conn, f.host)
	if err != nil {
		conn.Close()
		return err
	}
	defer c.Close()

	if err := c.Hello("gateway.email.svc.cluster.local"); err != nil {
		return fmt.Errorf("helo: %w", err)
	}
	// STARTTLS when offered (port 25/587 smarthosts usually offer it).
	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(&tls.Config{ServerName: f.host}); err != nil {
			return fmt.Errorf("starttls: %w", err)
		}
	}
	if err := c.Mail(from); err != nil {
		return fmt.Errorf("mail from: %w", err)
	}
	if err := c.Rcpt(m.To); err != nil {
		return fmt.Errorf("rcpt to: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(m.Raw); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
