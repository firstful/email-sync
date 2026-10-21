package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// D1Client stores raw mail blobs in Cloudflare D1 (SQLite) via the REST API.
//
// Why D1 and not R2: R2 is a paid product that must be enabled from the CF
// dashboard (API returns 10042); D1 provisions entirely by API. D1 gives
// 5 GB storage + 100k rows written/day on the free tier.
//
// Schema (created by the worker on first write):
//
//	raw_meta(guid TEXT PRIMARY KEY, size INT, chunks INT, received_at INT,
//	         from_addr TEXT, to_addr TEXT, subject TEXT)
//	raw_chunks(guid TEXT, seq INT, data TEXT, PRIMARY KEY(guid, seq))
//
// The body is stored base64-encoded and split into chunks so a single row
// stays well under D1's per-row size limit.
type D1Client struct {
	accountID string
	databaseID string
	token     string
	base      string
	http      *http.Client
}

func NewD1Client() *D1Client {
	acct := os.Getenv("CF_ACCOUNT_ID")
	db := os.Getenv("CF_D1_DATABASE_ID")
	token := os.Getenv("CF_API_TOKEN")
	if acct == "" || db == "" || token == "" {
		return nil
	}
	return &D1Client{
		accountID:  acct,
		databaseID: db,
		token:      token,
		base:       "https://api.cloudflare.com/client/v4",
		http:       &http.Client{Timeout: 120 * time.Second},
	}
}

func (d *D1Client) Enabled() bool { return d != nil }

// d1Result is the REST envelope for one query.
type d1Result struct {
	Success bool              `json:"success"`
	Errors  []json.RawMessage `json:"errors"`
	Result  []struct {
		Results []map[string]any `json:"results"`
	} `json:"result"`
}

// query runs a single SQL statement with optional bound params.
func (d *D1Client) query(ctx context.Context, sql string, params ...any) ([]map[string]any, error) {
	payload := map[string]any{"sql": sql}
	if len(params) > 0 {
		payload["params"] = params
	}
	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/accounts/%s/d1/database/%s/query", d.base, d.accountID, d.databaseID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("d1 query: %d %s", resp.StatusCode, string(raw))
	}
	var out d1Result
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("d1 parse: %w (%s)", err, string(raw)[:min(200, len(raw))])
	}
	if !out.Success {
		return nil, fmt.Errorf("d1 query failed: %s", string(raw)[:min(300, len(raw))])
	}
	if len(out.Result) == 0 {
		return nil, nil
	}
	return out.Result[0].Results, nil
}

// ChunkSize is the base64 chunk size used when writing blobs. 512 KiB of
// base64 is ~384 KiB of raw mail per row — comfortably under D1's per-row
// size limit while keeping the row count low.
const ChunkSize = 512 * 1024

// PutBlob stores raw bytes under a guid (chunked + meta row).
func (d *D1Client) PutBlob(ctx context.Context, guid string, raw []byte, fromAddr, toAddr, subject string) error {
	b64 := base64.StdEncoding.EncodeToString(raw)
	chunks := make([]string, 0, (len(b64)+ChunkSize-1)/ChunkSize)
	for i := 0; i < len(b64); i += ChunkSize {
		end := i + ChunkSize
		if end > len(b64) {
			end = len(b64)
		}
		chunks = append(chunks, b64[i:end])
	}
	for i, c := range chunks {
		if _, err := d.query(ctx,
			"INSERT OR REPLACE INTO raw_chunks (guid, seq, data) VALUES (?,?,?)",
			guid, i, c); err != nil {
			return err
		}
	}
	_, err := d.query(ctx,
		"INSERT OR REPLACE INTO raw_meta (guid, size, chunks, received_at, from_addr, to_addr, subject) VALUES (?,?,?,?,?,?,?)",
		guid, len(raw), len(chunks), time.Now().UnixMilli(), fromAddr, toAddr, subject)
	return err
}

// GetBlob reassembles and returns the raw bytes stored under a guid.
func (d *D1Client) GetBlob(ctx context.Context, guid string) ([]byte, error) {
	rows, err := d.query(ctx, "SELECT data FROM raw_chunks WHERE guid=? ORDER BY seq", guid)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("d1: no blob for guid %s", guid)
	}
	var sb strings.Builder
	for _, r := range rows {
		s, _ := r["data"].(string)
		sb.WriteString(s)
	}
	return base64.StdEncoding.DecodeString(sb.String())
}

// ListGUIDs returns every stored blob guid (boot reconciliation).
func (d *D1Client) ListGUIDs(ctx context.Context) ([]string, error) {
	rows, err := d.query(ctx, "SELECT guid FROM raw_meta ORDER BY received_at ASC")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if s, ok := r["guid"].(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// DeleteBlob removes the blob (cleanup after successful ingest).
func (d *D1Client) DeleteBlob(ctx context.Context, guid string) error {
	if _, err := d.query(ctx, "DELETE FROM raw_chunks WHERE guid=?", guid); err != nil {
		return err
	}
	_, err := d.query(ctx, "DELETE FROM raw_meta WHERE guid=?", guid)
	return err
}

// BlobMeta is the envelope metadata row.
type BlobMeta struct {
	GUID       string
	Size       int64
	Chunks     int
	ReceivedAt int64
	FromAddr   string
	ToAddr     string
	Subject    string
}

// Meta returns the metadata row for a guid.
func (d *D1Client) Meta(ctx context.Context, guid string) (*BlobMeta, error) {
	rows, err := d.query(ctx,
		"SELECT guid, size, chunks, received_at, from_addr, to_addr, subject FROM raw_meta WHERE guid=?", guid)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("d1: no meta for guid %s", guid)
	}
	r := rows[0]
	m := &BlobMeta{GUID: guid}
	if v, ok := r["size"].(float64); ok {
		m.Size = int64(v)
	}
	if v, ok := r["chunks"].(float64); ok {
		m.Chunks = int(v)
	}
	if v, ok := r["received_at"].(float64); ok {
		m.ReceivedAt = int64(v)
	}
	if v, ok := r["from_addr"].(string); ok {
		m.FromAddr = v
	}
	if v, ok := r["to_addr"].(string); ok {
		m.ToAddr = v
	}
	if v, ok := r["subject"].(string); ok {
		m.Subject = v
	}
	return m, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func atoiSafe(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}
