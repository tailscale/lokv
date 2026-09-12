// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package s3store

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/storetest"
)

type httpAPI struct {
	mu             sync.Mutex
	objects        map[string][]byte
	failures       map[string][]int
	puts           map[string]int
	listLimits     []int
	getKeys        []string
	badConditional bool
	cancel         context.CancelFunc
}

func newAPI() *httpAPI {
	return &httpAPI{objects: map[string][]byte{}, failures: map[string][]int{}, puts: map[string]int{}}
}

func (a *httpAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
	fail := func(status int) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		code := map[int]string{404: "NoSuchKey", 412: "PreconditionFailed", 409: "ConditionalRequestConflict", 403: "AccessDenied"}[status]
		fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test failure</Message></Error>", code)
	}
	switch r.Method {
	case http.MethodPut:
		a.puts[key]++
		if r.Header.Get("If-None-Match") != "*" {
			a.badConditional = true
			fail(403)
			return
		}
		if f := a.failures[key]; len(f) > 0 {
			a.failures[key] = f[1:]
			if a.cancel != nil {
				a.cancel()
			}
			fail(f[0])
			return
		}
		if _, ok := a.objects[key]; ok {
			fail(412)
			return
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			fail(403)
			return
		}
		a.objects[key] = b
		w.Header().Set("ETag", `"not-a-content-hash"`)
		w.WriteHeader(200)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			limit, _ := strconv.Atoi(r.URL.Query().Get("max-keys"))
			a.listLimits = append(a.listLimits, limit)
			if limit < 1 || limit > 1000 || r.URL.Query().Get("delimiter") != "" {
				fail(403)
				return
			}
			prefix := r.URL.Query().Get("prefix")
			token := r.URL.Query().Get("continuation-token")
			var keys []string
			for key := range a.objects {
				if strings.HasPrefix(key, prefix) && key > token {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			type item struct {
				Key string `xml:"Key"`
			}
			result := struct {
				XMLName               xml.Name `xml:"ListBucketResult"`
				Contents              []item   `xml:"Contents"`
				IsTruncated           bool     `xml:"IsTruncated"`
				NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
			}{IsTruncated: len(keys) > limit}
			keys = keys[:min(limit, len(keys))]
			for _, key := range keys {
				result.Contents = append(result.Contents, item{key})
			}
			if result.IsTruncated {
				result.NextContinuationToken = keys[len(keys)-1]
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(result)
			return
		}
		a.getKeys = append(a.getKeys, key)
		b, ok := a.objects[key]
		if !ok {
			fail(404)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		_, _ = w.Write(b)
	default:
		fail(403)
	}
}

func httpStore(t *testing.T, api *httpAPI) *Store {
	t.Helper()
	return storeForHandler(t, api)
}

func storeForHandler(t *testing.T, handler http.Handler) *Store {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	}), RetryMaxAttempts: 1, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired}, func(o *s3.Options) { o.BaseEndpoint = aws.String(server.URL); o.UsePathStyle = true })
	store, err := New(Config{Client: client, Bucket: "test-bucket", MaxConflictRetries: 2})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestHTTPStoreConformance(t *testing.T) {
	api := newAPI()
	store := httpStore(t, api)
	storetest.Test(t, store, "conformance/")
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.badConditional {
		t.Fatal("unconditional PUT sent")
	}
}

func TestHTTPHeadAndCarries(t *testing.T) {
	api := newAPI()
	store := httpStore(t, api)
	lg, err := lokv.Open[int](lokv.Config{Store: store, Prefix: "log"})
	if err != nil {
		t.Fatal(err)
	}
	var base *lokv.Snapshot[int]
	for i := 0; i < 257; i++ {
		base, err = lg.AppendTo(context.Background(), base, i, i+1000)
		if err != nil {
			t.Fatal(err)
		}
	}
	api.mu.Lock()
	api.getKeys = nil
	api.mu.Unlock()
	snap, err := lg.LoadHead(context.Background())
	if err != nil || snap.Revision() != 257 {
		t.Fatalf("head: %v", err)
	}
	if !slices.Equal(snap.Record().Value, []int{256, 1256}) {
		t.Fatalf("head batch: %v", snap.Record().Value)
	}
	seen := 0
	if err := lg.Scan(context.Background(), snap, lokv.All(), func(r lokv.Record[int]) error {
		if !slices.Equal(r.Value, []int{seen, seen + 1000}) {
			t.Fatalf("batch %d changed: %v", r.Revision, r.Value)
		}
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 257 {
		t.Fatalf("scan returned %d batches", seen)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.objects) != 274 {
		t.Fatalf("514 values in 257 batches created %d objects; want 257 commits and 17 segments", len(api.objects))
	}
	if len(api.getKeys) != 2 || !strings.Contains(api.getKeys[1], "/tree/2/") || !strings.HasSuffix(api.getKeys[1], ".json.zst") {
		t.Fatalf("cold scan GETs: %v", api.getKeys)
	}
	for _, limit := range api.listLimits {
		if limit != 1 {
			t.Fatal("HEAD LIST MaxKeys was not 1")
		}
	}
	if len(api.puts) != 274 || api.badConditional {
		t.Fatalf("creations %d conditional %v", len(api.puts), !api.badConditional)
	}
}

func TestHTTPConflictRetries(t *testing.T) {
	for _, tt := range []struct {
		name     string
		statuses []int
		want     error
		puts     int
	}{{"409 then success", []int{409}, nil, 2}, {"412", []int{412}, lokv.ErrExists, 1}, {"permanent", []int{403}, errors.New("error"), 1}, {"exhausted", []int{409, 409, 409, 409}, errors.New("error"), 3}} {
		t.Run(tt.name, func(t *testing.T) {
			api := newAPI()
			api.failures["key"] = tt.statuses
			store := httpStore(t, api)
			err := store.Create(context.Background(), "key", strings.NewReader("value"))
			if tt.want == nil && err != nil || tt.want != nil && err == nil {
				t.Fatal(err)
			}
			if tt.want == lokv.ErrExists && !errors.Is(err, lokv.ErrExists) {
				t.Fatal(err)
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			if api.puts["key"] != tt.puts || api.badConditional {
				t.Fatalf("puts %d conditional %v", api.puts["key"], !api.badConditional)
			}
		})
	}
}

func TestHTTPRetryCancellation(t *testing.T) {
	api := newAPI()
	api.failures["key"] = []int{409, 409}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api.cancel = cancel
	store := httpStore(t, api)
	if err := store.Create(ctx, "key", strings.NewReader("")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.puts["key"] != 1 {
		t.Fatal("retried after cancellation")
	}
}

func TestHTTPPagination(t *testing.T) {
	api := newAPI()
	for i := 0; i < 1005; i++ {
		api.objects[fmt.Sprintf("key%04d", i)] = nil
	}
	store := httpStore(t, api)
	keys, err := store.List(context.Background(), "key", 1003)
	if err != nil || len(keys) != 1003 || keys[1002] != "key1002" {
		t.Fatalf("list count %d: %v", len(keys), err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if fmt.Sprint(api.listLimits) != "[1000 3]" {
		t.Fatal(api.listLimits)
	}
}

type fakeClient struct {
	Client
	get  func(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	list func(context.Context, *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	put  func(context.Context, *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

func (f *fakeClient) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return f.get(ctx, in)
}

func (f *fakeClient) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return f.list(ctx, in)
}

func (f *fakeClient) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return f.put(ctx, in)
}

type readerAtOnly struct {
	io.ReaderAt
	size int64
}

func (r readerAtOnly) Size() int64 { return r.size }

type conditionalConflict struct{}

func (conditionalConflict) Error() string       { return "conditional conflict" }
func (conditionalConflict) HTTPStatusCode() int { return 409 }

func TestReaderAtUploadReplay(t *testing.T) {
	const value = "the complete upload body"
	attempts := 0
	client := &fakeClient{put: func(_ context.Context, in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		attempts++
		if aws.ToInt64(in.ContentLength) != int64(len(value)) || aws.ToString(in.IfNoneMatch) != "*" {
			t.Fatal("incorrect length or conditional header")
		}
		if attempts == 1 {
			if _, err := io.CopyN(io.Discard, in.Body, 5); err != nil {
				t.Fatal(err)
			}
			return nil, conditionalConflict{}
		}
		body, err := io.ReadAll(in.Body)
		if err != nil || string(body) != value {
			t.Fatalf("retry body = %q, %v", body, err)
		}
		return &s3.PutObjectOutput{}, nil
	}}
	store, err := New(Config{Client: client, Bucket: "test-bucket"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), "key", readerAtOnly{strings.NewReader(value), int64(len(value))}); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

func TestGetWithoutContentLength(t *testing.T) {
	body := &trackedBody{Reader: strings.NewReader("123456789")}
	client := &fakeClient{get: func(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: body}, nil
	}}
	store, err := New(Config{Client: client, Bucket: "test-bucket"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Get(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	if body.closed {
		t.Fatal("body closed before caller could read it")
	}
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "123456789" {
		t.Fatalf("Read = %q, %v", got, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !body.closed {
		t.Fatal("body leaked")
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, bucket := range []string{"", "a", "UPPERCASE", "dir--x-s3", "a/b", "-bucket", "bucket-", "a..b", "arn:aws:s3:::bucket"} {
		if _, err := New(Config{Client: &fakeClient{}, Bucket: bucket}); err == nil {
			t.Fatalf("accepted bucket %q", bucket)
		}
	}
	if _, err := New(Config{Bucket: "test-bucket"}); err == nil {
		t.Fatal("nil client accepted")
	}
}

func TestTypedNilAndReservedBuckets(t *testing.T) {
	var nilClient *fakeClient
	if _, err := New(Config{Client: nilClient, Bucket: "test-bucket"}); err == nil {
		t.Fatal("typed nil accepted")
	}
	for _, bucket := range []string{"127.0.0.1", "access-point-s3alias", "object-lambda--ol-s3", "table--table-s3"} {
		if _, err := New(Config{Client: &fakeClient{}, Bucket: bucket}); err == nil {
			t.Fatalf("accepted %q", bucket)
		}
	}
}
