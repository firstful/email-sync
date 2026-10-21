package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// R2Client talks to Cloudflare R2 through the Cloudflare REST API.
//
// No S3/SigV4 needed: a plain CF API token carrying the
// "Workers R2 Storage Read/Write" permission groups is sufficient for
// object put/get/delete/list. That keeps the service dependency-light.
//
// R2 is the durable source of truth for inbound mail. The Queue is only a
// work signal (it expires), so R2 objects outlive both queue messages and
// homelab downtime.
type R2Client struct {
	accountID string
	bucket    string
	token     string
	base      string
	http      *http.Client
}

func NewR2Client() *R2Client {
	acct := os.Getenv("CF_ACCOUNT_ID")
	bucket := os.Getenv("CF_R2_BUCKET")
	token := os.Getenv("CF_API_TOKEN")
	if acct == "" || bucket == "" || token == "" {
		return nil
	}
	return &R2Client{
		accountID: acct,
		bucket:    bucket,
		token:     token,
		base:      "https://api.cloudflare.com/client/v4",
		http:      &http.Client{Timeout: 120 * time.Second},
	}
}

func (r *R2Client) Enabled() bool { return r != nil }

func (r *R2Client) objectURL(key string) string {
	return fmt.Sprintf("%s/accounts/%s/r2/buckets/%s/objects/%s",
		r.base, r.accountID, r.bucket, url.PathEscape(key))
}

// PutObject stores raw bytes under key.
func (r *R2Client) PutObject(ctx context.Context, key string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.objectURL(key),
		bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Content-Type", "message/rfc822")
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return fmt.Errorf("r2 put %s: %d %s", key, resp.StatusCode, string(b))
	}
	return nil
}

// FetchObject returns the raw bytes of an R2 object.
func (r *R2Client) FetchObject(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.objectURL(key), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, &R2NotFoundError{Key: key}
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return nil, fmt.Errorf("r2 fetch %s: %d %s", key, resp.StatusCode, string(b))
	}
	return io.ReadAll(resp.Body)
}

// R2NotFoundError signals the object does not exist. This is terminal for a
// queue message: the blob was already ingested and cleaned up (or the queue
// message outlived its object), so the message must be acked, not retried.
type R2NotFoundError struct {
	Key string
}

func (e *R2NotFoundError) Error() string {
	return fmt.Sprintf("r2: object not found: %s", e.Key)
}

// DeleteObject removes an R2 object (best-effort cleanup after ingest).
func (r *R2Client) DeleteObject(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.objectURL(key), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("r2 delete %s: %d", key, resp.StatusCode)
	}
	return nil
}

// R2Object is a listing entry.
type R2Object struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// ListObjects returns all object keys (cursor-paginated).
//
// The REST list endpoint returns a flat `result` array of objects, with
// pagination via `result_info.cursor` (NOT result.objects / result.truncated).
func (r *R2Client) ListObjects(ctx context.Context) ([]R2Object, error) {
	var out []R2Object
	cursor := ""
	for {
		u := fmt.Sprintf("%s/accounts/%s/r2/buckets/%s/objects?per_page=1000",
			r.base, r.accountID, r.bucket)
		if cursor != "" {
			u += "&cursor=" + url.QueryEscape(cursor)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+r.token)
		resp, err := r.http.Do(req)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("r2 list: %d %s", resp.StatusCode, string(body))
		}
		var parsed struct {
			Result     []R2Object `json:"result"`
			ResultInfo struct {
				Cursor string `json:"cursor"`
			} `json:"result_info"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("r2 list parse: %w (%s)", err, string(body)[:min(200, len(body))])
		}
		out = append(out, parsed.Result...)
		cursor = parsed.ResultInfo.Cursor
		if cursor == "" {
			break
		}
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
