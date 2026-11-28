package main

import (
	"fmt"
	"testing"
)

// Issue #35: not-found mapping must be typed. A transient catalog
// failure must never masquerade as "doesn't exist" — DeleteObject would
// 204 on nothing, and reads would 404 on fully real objects.
func TestNotFoundOrMapping(t *testing.T) {
	err := notFoundOr(fmt.Errorf("lookup: %w", ErrNotFound), "NoSuchKey", "key does not exist")
	se, ok := err.(*errS3T)
	if !ok {
		t.Fatalf("expected *errS3T, got %T", err)
	}
	if se.code != "NoSuchKey" || se.status != 404 {
		t.Fatalf("ErrNotFound: got code=%q status=%d", se.code, se.status)
	}

	err = notFoundOr(fmt.Errorf("dial tcp: connection refused"), "NoSuchKey", "key does not exist")
	se, ok = err.(*errS3T)
	if !ok {
		t.Fatalf("expected errS3T, got %T", err)
	}
	if se.code != "InternalError" || se.status != 500 {
		t.Fatalf("transient: got code=%q status=%d", se.code, se.status)
	}
}
