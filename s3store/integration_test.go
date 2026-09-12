// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package s3store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/s3store"
	"github.com/tailscale/lokv/storetest"
)

// This opt-in test writes permanent objects to a unique prefix. It never deletes.
func TestLiveS3(t *testing.T) {
	if os.Getenv("LOKV_S3_INTEGRATION") != "1" {
		t.Skip("set LOKV_S3_INTEGRATION=1 and LOKV_S3_BUCKET to run")
	}
	bucket := os.Getenv("LOKV_S3_BUCKET")
	if bucket == "" {
		t.Fatal("LOKV_S3_BUCKET is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	store, err := s3store.New(s3store.Config{Client: s3.NewFromConfig(cfg), Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	prefix := "lokv-integration/" + hex.EncodeToString(id[:]) + "/"
	t.Logf("permanent test objects: s3://%s/%s", bucket, prefix)
	storetest.Test(t, store, prefix+"store/")
	lg, err := lokv.Open[int](lokv.Config{Store: store, Prefix: prefix + "log"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 17; i++ {
		r, err := lg.Append(ctx, i)
		if err != nil || r.Revision != int64(i+1) {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	keys, err := store.List(ctx, prefix+"log/v1/log/", 1)
	if err != nil || len(keys) != 1 || keys[0] != prefix+"log/v1/log/7fffffffffffffee.json" {
		t.Fatalf("one-key LIST: %v %v", keys, err)
	}
	if err := store.Create(ctx, keys[0], bytes.NewReader([]byte("overwrite"))); !errors.Is(err, lokv.ErrExists) {
		t.Fatalf("conditional duplicate PUT: %v", err)
	}
	snap, err := lg.LoadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := lg.Verify(ctx, snap); err != nil {
		t.Fatal(err)
	}
}
