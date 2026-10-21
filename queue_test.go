package main

import (
	"encoding/json"
	"testing"
)

// The worker writes "<guid>.eml" to R2; the queue envelope carries the bare
// GUID. If these two disagree, every queue-delivered message 404s and mail
// silently piles up in R2. This test pins the mapping.
func TestR2KeyForGUID(t *testing.T) {
	guid := "0ea98b95-4764-4ece-b4b5-d612c94ed9d3"
	key := r2KeyForGUID(guid)
	if key != guid+".eml" {
		t.Fatalf("r2KeyForGUID = %q, want %q", key, guid+".eml")
	}
	if got := guidFromR2Key(key); got != guid {
		t.Fatalf("guidFromR2Key = %q, want %q", got, guid)
	}
}

func TestGUIDRoundTrip(t *testing.T) {
	for _, guid := range []string{
		"0ea98b95-4764-4ece-b4b5-d612c94ed9d3",
		"11111111-2222-3333-4444-555555555555",
		"cd84e6ef-0b43-4473-8003-1952ca1c234f",
	} {
		if got := guidFromR2Key(r2KeyForGUID(guid)); got != guid {
			t.Errorf("round trip failed for %s: got %s", guid, got)
		}
	}
}

// The queue API returns the message body as a JSON *string* containing our
// JSON object. Go's json.Unmarshal unescapes that string, so m.Body arrives
// as the raw object text — but a still-quoted body must also parse.
func TestParseEnvelope(t *testing.T) {
	inner := `{"guid":"abc-123","to":"a@ffilt.us","from":"b@c.com","subject":"hi","date":"now"}`
	// Body as raw object text (what Go's unmarshal yields from the API).
	got, err := parseEnvelope(inner)
	if err != nil {
		t.Fatalf("object body: %v", err)
	}
	if got.GUID != "abc-123" || got.To != "a@ffilt.us" {
		t.Fatalf("object body parsed wrong: %+v", got)
	}
	// Body still wrapped in quotes (defensive path).
	quoted, _ := json.Marshal(inner)
	got2, err := parseEnvelope(string(quoted))
	if err != nil {
		t.Fatalf("quoted body: %v", err)
	}
	if got2.GUID != "abc-123" {
		t.Fatalf("quoted body parsed wrong: %+v", got2)
	}
	// Missing guid is an error.
	if _, err := parseEnvelope(`{"to":"a@ffilt.us"}`); err == nil {
		t.Fatal("expected error for envelope without guid")
	}
}
