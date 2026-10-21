package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// QueueClient talks to a Cloudflare Queue via the HTTP pull consumer API.
type QueueClient struct {
	accountID string
	queueID   string
	token     string
	base      string
	http      *http.Client
}

func NewQueueClient() *QueueClient {
	acct := os.Getenv("CF_ACCOUNT_ID")
	qid := os.Getenv("CF_QUEUE_ID")
	token := os.Getenv("CF_API_TOKEN")
	if acct == "" || qid == "" || token == "" {
		return nil
	}
	return &QueueClient{
		accountID: acct,
		queueID:   qid,
		token:     token,
		base:      "https://api.cloudflare.com/client/v4",
		http:      &http.Client{Timeout: 30 * time.Second},
	}
}

func (q *QueueClient) Enabled() bool { return q != nil }

// queueMessage is one pulled message. The API returns `body` as a JSON
// *string* containing our JSON object — not as a nested object.
type queueMessage struct {
	ID      string `json:"id"`
	Body    string `json:"body"`
	LeaseID string `json:"lease_id"`
}

// Pull fetches up to batchSize messages.
func (q *QueueClient) Pull(ctx context.Context, batchSize int) ([]queueMessage, error) {
	body, _ := json.Marshal(map[string]any{
		"visibility_timeout_ms": 60000,
		"batch_size":            batchSize,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/accounts/%s/queues/%s/messages/pull", q.base, q.accountID, q.queueID),
		strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+q.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("queue pull: %d %s", resp.StatusCode, raw)
	}
	var parsed struct {
		Result struct {
			Messages []queueMessage `json:"messages"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("queue pull parse: %w", err)
	}
	return parsed.Result.Messages, nil
}

// Ack acknowledges a batch of messages by lease id.
func (q *QueueClient) Ack(ctx context.Context, leaseIDs []string) error {
	if len(leaseIDs) == 0 {
		return nil
	}
	acks := make([]map[string]string, 0, len(leaseIDs))
	for _, id := range leaseIDs {
		acks = append(acks, map[string]string{"lease_id": id})
	}
	body, _ := json.Marshal(map[string]any{"acks": acks})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/accounts/%s/queues/%s/messages/ack", q.base, q.accountID, q.queueID),
		strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+q.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("queue ack: %d", resp.StatusCode)
	}
	return nil
}

// QueuedEnvelope is the small payload the worker enqueues.
type QueuedEnvelope struct {
	GUID    string `json:"guid"`
	To      string `json:"to"`
	From    string `json:"from"`
	Subject string `json:"subject"`
	Date    string `json:"date"`
}

// parseEnvelope unwraps the queue body (a JSON string) into QueuedEnvelope.
func parseEnvelope(body string) (QueuedEnvelope, error) {
	var env QueuedEnvelope
	b := strings.TrimSpace(body)
	if b == "" {
		return env, fmt.Errorf("empty body")
	}
	// The API hands back the body as a JSON string; unquote it first.
	if strings.HasPrefix(b, `"`) {
		var inner string
		if err := json.Unmarshal([]byte(b), &inner); err == nil {
			b = inner
		}
	}
	if err := json.Unmarshal([]byte(b), &env); err != nil {
		return env, fmt.Errorf("envelope parse: %w", err)
	}
	if env.GUID == "" {
		return env, fmt.Errorf("envelope missing guid")
	}
	return env, nil
}

// Drainer orchestrates: pull queue -> fetch R2 blob -> store -> rules -> cleanup -> ack.
type Drainer struct {
	store    *Store
	rules    *RuleEngine
	r2       *R2Client
	queue    *QueueClient
	interval time.Duration
}

func NewDrainer(store *Store, rules *RuleEngine, r2 *R2Client, queue *QueueClient) *Drainer {
	iv := 15 * time.Second
	if v := os.Getenv("DRAIN_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			iv = d
		}
	}
	return &Drainer{store: store, rules: rules, r2: r2, queue: queue, interval: iv}
}

// Run loops: boot reconciliation (R2) then periodic queue drain.
func (d *Drainer) Run(ctx context.Context) {
	// Boot reconciliation: process any blobs in R2 not yet in the local DB.
	// This is the durability path — queue messages expire, R2 objects persist,
	// so a long homelab outage loses nothing.
	if d.r2 != nil {
		d.reconcileR2(ctx)
	}
	t := time.NewTicker(d.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.drainOnce(ctx)
		}
	}
}

// reconcileR2 processes leftover blobs (covers expired queue messages).
func (d *Drainer) reconcileR2(ctx context.Context) {
	objs, err := d.r2.ListObjects(ctx)
	if err != nil {
		log.Printf("drainer: r2 list failed: %v", err)
		return
	}
	log.Printf("drainer: reconciling %d r2 object(s)", len(objs))
	for _, o := range objs {
		d.processKey(ctx, o.Key)
	}
}

// drainOnce pulls a batch from the queue and processes each.
func (d *Drainer) drainOnce(ctx context.Context) {
	if d.queue == nil {
		return
	}
	msgs, err := d.queue.Pull(ctx, 10)
	if err != nil {
		log.Printf("drainer: queue pull failed: %v", err)
		return
	}
	if len(msgs) == 0 {
		return
	}
	leaseIDs := make([]string, 0, len(msgs))
	for _, m := range msgs {
		env, err := parseEnvelope(m.Body)
		if err != nil {
			log.Printf("drainer: bad envelope: %v", err)
			leaseIDs = append(leaseIDs, m.LeaseID) // drop it, don't retry forever
			continue
		}
		d.processKey(ctx, env.GUID)
		leaseIDs = append(leaseIDs, m.LeaseID)
	}
	if err := d.queue.Ack(ctx, leaseIDs); err != nil {
		log.Printf("drainer: ack failed: %v", err)
	}
}

// processKey fetches, stores, triggers, and cleans up one message by R2 key.
func (d *Drainer) processKey(ctx context.Context, key string) {
	if key == "" || d.r2 == nil {
		return
	}
	guid := strings.TrimSuffix(key, ".eml")
	// Dedup: skip if already stored locally.
	if dup, _ := d.store.HasGUID(ctx, guid); dup {
		// Already ingested — clean up the staging copy.
		if err := d.r2.DeleteObject(ctx, key); err != nil {
			log.Printf("drainer: r2 delete %s: %v", key, err)
		}
		return
	}
	raw, err := d.r2.FetchObject(ctx, key)
	if err != nil {
		log.Printf("drainer: fetch %s failed: %v", key, err)
		return
	}
	msg, err := parseRawMIME(raw)
	if err != nil {
		log.Printf("drainer: parse %s failed: %v", key, err)
		return
	}
	if err := d.store.InsertIngestGUID(ctx, msg, guid); err != nil {
		log.Printf("drainer: store %s failed: %v", key, err)
		return
	}
	// Rule engine: metadata-only dispatch (untrusted content never rides along).
	if d.rules != nil {
		d.rules.Dispatch(msg)
	}
	// Best-effort cleanup of the staging blob. If this fails the object stays
	// in R2 and boot reconciliation will find it (and skip it as a dup).
	if err := d.r2.DeleteObject(ctx, key); err != nil {
		log.Printf("drainer: r2 delete %s: %v", key, err)
	}
	log.Printf("drainer: processed %s (uid=%d, from=%s)", key, msg.UID, msg.FromAddr)
}
