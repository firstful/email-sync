package main

import (
	"path/filepath"
	"testing"
	"time"
)

// TestCullableObjectsAgeBoundary checks the age cutoff logic in isolation:
// an object younger than maxAge must survive, one older must be culled,
// and one with no timestamp must never be culled blind.
func TestCullableObjectsAgeBoundary(t *testing.T) {
	now := time.Now()
	objs := []R2Object{
		{Key: "fresh.eml", LastModified: now.Add(-1 * time.Hour)},
		{Key: "stale.eml", LastModified: now.Add(-15 * 24 * time.Hour)},
		{Key: "nots.eml", LastModified: time.Time{}},
	}

	got := map[string]bool{}
	for _, k := range cullableObjects(objs, 14*24*time.Hour, now) {
		got[k] = true
	}
	if got["fresh.eml"] {
		t.Error("fresh.eml (1h old) must NOT be culled")
	}
	if !got["stale.eml"] {
		t.Error("stale.eml (15d old) MUST be culled")
	}
	if got["nots.eml"] {
		t.Error("nots.eml (no timestamp) must NOT be culled — never cull blind")
	}
}

// TestCullableObjectsExactBoundary pins the 14-day edge: an object exactly
// at the cutoff is culled, one a second newer is not.
func TestCullableObjectsExactBoundary(t *testing.T) {
	now := time.Now()
	objs := []R2Object{
		{Key: "exact.eml", LastModified: now.Add(-14 * 24 * time.Hour)},
		{Key: "newer.eml", LastModified: now.Add(-14*24*time.Hour + time.Second)},
	}
	got := map[string]bool{}
	for _, k := range cullableObjects(objs, 14*24*time.Hour, now) {
		got[k] = true
	}
	if !got["exact.eml"] {
		t.Error("exact.eml (exactly 14d old) must be culled")
	}
	if got["newer.eml"] {
		t.Error("newer.eml (14d minus 1s) must NOT be culled")
	}
}

// TestDefaultMaxAge pins the 2-week default.
func TestDefaultMaxAge(t *testing.T) {
	if DefaultMaxAge != 14*24*time.Hour {
		t.Errorf("DefaultMaxAge = %v, want %v (14 days)", DefaultMaxAge, 14*24*time.Hour)
	}
}

// TestR2MaxAgeEnvOverride checks R2_MAX_AGE is honored.
func TestR2MaxAgeEnvOverride(t *testing.T) {
	t.Setenv("R2_MAX_AGE", "48h")
	store, err := NewStore(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	d := NewDrainer(store, nil, nil, nil, nil)
	if d.maxAge != 48*time.Hour {
		t.Errorf("maxAge = %v, want 48h", d.maxAge)
	}
}

// TestR2MaxAgeBadEnvFallsBack checks a malformed value keeps the default.
func TestR2MaxAgeBadEnvFallsBack(t *testing.T) {
	t.Setenv("R2_MAX_AGE", "not-a-duration")
	store, err := NewStore(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	d := NewDrainer(store, nil, nil, nil, nil)
	if d.maxAge != DefaultMaxAge {
		t.Errorf("maxAge = %v, want default %v", d.maxAge, DefaultMaxAge)
	}
}
