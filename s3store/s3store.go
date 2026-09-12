// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package s3store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/tailscale/lokv"
)

// Client is the subset of the AWS SDK v2 client required by this adapter.
type Client interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Config contains backend settings; AWS credentials, region, and general
// transport retry policy are configured on Client.
type Config struct {
	Client             Client
	Bucket             string
	MaxConflictRetries int // Retries HTTP 409 conditional conflicts; default 8.
}

// Store implements lokv.Store without update, delete, or multipart methods.
type Store struct {
	client  Client
	bucket  string
	retries int
}

var _ lokv.Store = (*Store)(nil)

// New validates configuration without making a network request. Bucket must be
// a general-purpose bucket name, not an ARN, URL, or directory bucket name.
func New(cfg Config) (*Store, error) {
	if cfg.Client == nil {
		return nil, errors.New("s3store: nil client")
	}
	v := reflect.ValueOf(cfg.Client)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		if v.IsNil() {
			return nil, errors.New("s3store: nil client")
		}
	}
	b := cfg.Bucket
	if len(b) < 3 || len(b) > 63 || strings.HasSuffix(b, "--x-s3") || strings.Contains(b, "..") || b[0] == '.' || b[0] == '-' || b[len(b)-1] == '.' || b[len(b)-1] == '-' {
		return nil, errors.New("s3store: invalid general-purpose bucket name")
	}
	if _, err := netip.ParseAddr(b); err == nil {
		return nil, errors.New("s3store: bucket name cannot be an IP address")
	}
	for _, suffix := range []string{"-s3alias", "--ol-s3", "--table-s3"} {
		if strings.HasSuffix(b, suffix) {
			return nil, errors.New("s3store: bucket aliases are unsupported")
		}
	}
	for _, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '-') {
			return nil, errors.New("s3store: invalid bucket name")
		}
	}
	if cfg.MaxConflictRetries < 0 {
		return nil, errors.New("s3store: invalid limit")
	}
	if cfg.MaxConflictRetries == 0 {
		cfg.MaxConflictRetries = 8
	}
	return &Store{cfg.Client, b, cfg.MaxConflictRetries}, nil
}

func (s *Store) wrap(op, key string, err error) error {
	return fmt.Errorf("s3store: %s bucket %s key %s: %w", op, s.bucket, key, err)
}

func status(err error) int {
	var e interface{ HTTPStatusCode() int }
	if errors.As(err, &e) {
		return e.HTTPStatusCode()
	}
	return 0
}

func code(err error) string {
	var e smithy.APIError
	if errors.As(err, &e) {
		return e.ErrorCode()
	}
	return ""
}

// List returns ascending keys. Requests of up to 1000 keys use one S3 request;
// larger limits use continuation tokens while preserving global ordering.
func (s *Store) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, errors.New("s3store: LIST limit must be positive")
	}
	var keys []string
	var token *string
	for len(keys) < limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n := min(limit-len(keys), 1000)
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(int32(n)), ContinuationToken: token})
		if err != nil {
			return nil, s.wrap("list", prefix, err)
		}
		if out == nil || len(out.Contents) > n {
			return nil, s.wrap("list", prefix, lokv.ErrCorrupt)
		}
		for _, obj := range out.Contents {
			if obj.Key == nil || !strings.HasPrefix(*obj.Key, prefix) || len(keys) > 0 && keys[len(keys)-1] >= *obj.Key {
				return nil, s.wrap("unordered list", prefix, lokv.ErrCorrupt)
			}
			keys = append(keys, *obj.Key)
		}
		if !aws.ToBool(out.IsTruncated) || len(keys) == limit {
			break
		}
		if len(out.Contents) == 0 || out.NextContinuationToken == nil || *out.NextContinuationToken == "" || token != nil && *token == *out.NextContinuationToken {
			return nil, s.wrap("invalid pagination", prefix, lokv.ErrCorrupt)
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

// Get opens an S3 response body. The caller must close it.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if status(err) == 404 || code(err) == "NoSuchKey" {
			err = fmt.Errorf("%w: %w", lokv.ErrNotFound, err)
		}
		return nil, s.wrap("get", key, err)
	}
	if out == nil || out.Body == nil {
		return nil, s.wrap("get", key, lokv.ErrCorrupt)
	}
	return &responseBody{s: s, ctx: ctx, key: key, body: out.Body}, nil
}

// Create only performs conditional, single-request PUTs. HTTP 409 is retried
// with bounded jitter; HTTP 412 maps to ErrExists. Other transport retries are
// handled by the configured AWS SDK retryer.
func (s *Store) Create(ctx context.Context, key string, value lokv.SizeReaderAt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value == nil || value.Size() < 0 {
		return errors.New("s3store: invalid value size")
	}
	size := value.Size()
	contentType := "application/json"
	if strings.HasSuffix(key, ".zst") {
		contentType = "application/zstd"
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: io.NewSectionReader(value, 0, size), ContentLength: aws.Int64(size), ContentType: aws.String(contentType), IfNoneMatch: aws.String("*")})
		if err == nil {
			return nil
		}
		if status(err) == 412 || code(err) == "PreconditionFailed" {
			return s.wrap("put", key, fmt.Errorf("%w: %w", lokv.ErrExists, err))
		}
		if status(err) != 409 && code(err) != "ConditionalRequestConflict" || attempt >= s.retries {
			return s.wrap("put", key, err)
		}
		var jitter [1]byte
		_, _ = rand.Read(jitter[:])
		delay := (10 * time.Millisecond << min(attempt, 6)) * time.Duration(128+int(jitter[0])) / 256
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// responseBody keeps read and close failures associated with their S3 key.
type responseBody struct {
	s    *Store
	ctx  context.Context
	key  string
	body io.ReadCloser
}

func (b *responseBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := b.body.Read(p)
	if err != nil && err != io.EOF {
		err = b.s.wrap("read", b.key, err)
	}
	return n, err
}

func (b *responseBody) Close() error {
	if err := b.body.Close(); err != nil {
		return b.s.wrap("close", b.key, err)
	}
	return nil
}
