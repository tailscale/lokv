// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/tailscale/lokv"
	"github.com/tailscale/lokv/storetest"
)

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<Error><Code>%s</Code><Message>injected failure</Message></Error>", code)
}

func isComplete(r *http.Request) (ok bool) {
	return r.Method == http.MethodPost && r.URL.Query().Has("uploadId")
}

// fakeS3Store uses the real SDK and gofakes3's multipart implementation.
// gofakes3 v1.2.0 ignores conditions on CompleteMultipartUpload, so this shim
// serializes publications and adds the missing precondition check. It does not
// emulate multipart transfer, assembly, listing, or aborts.
func fakeS3Store(t *testing.T, wrap func(http.Handler) http.Handler) *Store {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	fake := gofakes3.New(backend).Server()
	var publishMu sync.Mutex
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		complete := isComplete(r)
		put := r.Method == http.MethodPut && !r.URL.Query().Has("uploadId")
		if complete || put {
			publishMu.Lock()
			defer publishMu.Unlock()
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("publication without If-None-Match: *")
				s3Error(w, 403, "AccessDenied")
				return
			}
			if complete {
				key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
				if _, err := backend.HeadObject("test-bucket", key); err == nil {
					s3Error(w, 412, "PreconditionFailed")
					return
				} else if !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey) {
					t.Errorf("checking completion precondition: %v", err)
					s3Error(w, 500, "InternalError")
					return
				}
			}
		}
		fake.ServeHTTP(w, r)
	})
	if wrap != nil {
		handler = wrap(handler)
	}
	return storeForHandler(t, handler)
}

func assertNoUploads(t *testing.T, store *Store) {
	t.Helper()
	client := store.client.(*s3.Client)
	out, err := client.ListMultipartUploads(t.Context(), &s3.ListMultipartUploadsInput{Bucket: aws.String(store.bucket)})
	// gofakes3 returns NoSuchUpload when this bucket has never had an upload.
	if code(err) == "NoSuchUpload" {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Uploads) != 0 {
		t.Fatalf("leaked %d multipart uploads", len(out.Uploads))
	}
}

func readObject(t *testing.T, store *Store, key string) []byte {
	t.Helper()
	body, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if err := errors.Join(err, body.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestFakeS3Conformance(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		t.Run(fmt.Sprintf("multipart=%v", multipart), func(t *testing.T) {
			store := fakeS3Store(t, nil)
			if multipart {
				store.multipartThreshold = 0
			}
			storetest.Test(t, store, "conformance/")
			assertNoUploads(t, store)
		})
	}
}

func TestMultipartThreshold(t *testing.T) {
	const threshold = 10
	var puts, completes atomic.Int32
	store := fakeS3Store(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isComplete(r) {
				completes.Add(1)
			} else if r.Method == http.MethodPut && !r.URL.Query().Has("uploadId") {
				puts.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	})
	store.multipartThreshold = threshold
	for _, size := range []int{0, threshold - 1, threshold, threshold + 1} {
		key := fmt.Sprint(size)
		data := strings.Repeat("x", size)
		if err := store.Create(t.Context(), key, strings.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if string(readObject(t, store, key)) != data {
			t.Fatalf("size %d: wrong contents", size)
		}
	}
	if puts.Load() != 3 || completes.Load() != 1 {
		t.Fatalf("PUTs=%d completions=%d", puts.Load(), completes.Load())
	}
	assertNoUploads(t, store)
}

func enableSDKRetries(store *Store) {
	store.client = s3.New(store.client.(*s3.Client).Options(), func(o *s3.Options) {
		o.RetryMaxAttempts = 2
		o.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 2
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		})
	})
}

func TestMultipartPartsAndReplay(t *testing.T) {
	var part2Attempts atomic.Int32
	store := fakeS3Store(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Query().Get("partNumber") == "2" && part2Attempts.Add(1) == 1 {
				// Consume the request before asking the SDK to replay this part.
				io.Copy(io.Discard, r.Body)
				s3Error(w, 500, "InternalError")
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	enableSDKRetries(store)
	store.multipartThreshold = 0
	store.multipartPartSize = minPartSize
	data := make([]byte, 2*minPartSize+123)
	for i := range data {
		data[i] = byte(i*37 + i/minPartSize)
	}
	if err := store.Create(t.Context(), "parts.zst", readerAtOnly{bytes.NewReader(data), int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, store, "parts.zst"); !bytes.Equal(got, data) {
		t.Fatal("multipart upload or part replay changed the body")
	}
	if part2Attempts.Load() != 2 {
		t.Fatalf("part 2 attempts: %d", part2Attempts.Load())
	}
	out, err := store.client.(*s3.Client).HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: aws.String(store.bucket), Key: aws.String("parts.zst")})
	if err != nil || aws.ToString(out.ContentType) != "application/zstd" {
		t.Fatalf("content type: %v, %v", out, err)
	}
	assertNoUploads(t, store)
}

func TestMultipartFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       string
		status      int
		code        string
		failures    int
		wantSuccess bool
		wantExists  bool
		wantUploads int32
		wantAborts  int32
	}{
		{"initiation", "initiate", 403, "AccessDenied", 1, false, false, 1, 0},
		{"part", "part", 403, "AccessDenied", 1, false, false, 1, 1},
		{"completion", "complete", 403, "AccessDenied", 1, false, false, 1, 1},
		{"embedded error", "complete", 200, "InternalError", 1, false, false, 1, 1},
		{"exists", "complete", 412, "PreconditionFailed", 1, false, true, 1, 1},
		{"conflict restart", "complete", 409, "ConditionalRequestConflict", 1, true, false, 2, 1},
		{"conflict exhausted", "complete", 409, "ConditionalRequestConflict", 3, false, false, 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var uploads, aborts, failures atomic.Int32
			store := fakeS3Store(t, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var phase string
					switch {
					case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
						phase = "initiate"
						uploads.Add(1)
					case r.Method == http.MethodPut:
						phase = "part"
					case isComplete(r):
						phase = "complete"
					case r.Method == http.MethodDelete:
						aborts.Add(1)
					}
					if phase == tc.phase && failures.Add(1) <= int32(tc.failures) {
						s3Error(w, tc.status, tc.code)
						return
					}
					next.ServeHTTP(w, r)
				})
			})
			store.multipartThreshold = 0
			err := store.Create(t.Context(), "key", strings.NewReader("value"))
			if (err == nil) != tc.wantSuccess || errors.Is(err, lokv.ErrExists) != tc.wantExists {
				t.Fatalf("Create: %v", err)
			}
			if uploads.Load() != tc.wantUploads || aborts.Load() != tc.wantAborts {
				t.Fatalf("uploads=%d aborts=%d; want %d, %d", uploads.Load(), aborts.Load(), tc.wantUploads, tc.wantAborts)
			}
			if tc.wantSuccess {
				if string(readObject(t, store, "key")) != "value" {
					t.Fatal("wrong contents after restart")
				}
			} else if body, err := store.Get(t.Context(), "key"); !errors.Is(err, lokv.ErrNotFound) {
				if body != nil {
					body.Close()
				}
				t.Fatalf("published failed upload: %v", err)
			}
			assertNoUploads(t, store)
		})
	}
}

func TestMultipartLogAndLostCompletion(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost=%v", lost), func(t *testing.T) {
			var completions atomic.Int32
			store := fakeS3Store(t, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if isComplete(r) {
						completions.Add(1)
						if lost {
							result := httptest.NewRecorder()
							next.ServeHTTP(result, r)
							if result.Code != 200 {
								t.Errorf("completion failed: %s", result.Body)
							}
							// The object is published, but its response never arrives.
							conn, _, err := w.(http.Hijacker).Hijack()
							if err != nil {
								t.Error(err)
								return
							}
							conn.Close()
							return
						}
					}
					next.ServeHTTP(w, r)
				})
			})
			store.multipartThreshold = 0
			lg, err := lokv.Open[int](lokv.Config{Store: store, Prefix: "log"})
			if err != nil {
				t.Fatal(err)
			}
			var snap *lokv.Snapshot[int]
			for i := 1; i <= 17; i++ {
				snap, err = lg.AppendTo(t.Context(), snap, i, -i)
				if err != nil {
					t.Fatal(err)
				}
			}
			snap, err = lg.LoadHead(t.Context())
			if err != nil || snap.Revision() != 17 {
				t.Fatalf("LoadHead: %v, %v", snap, err)
			}
			var seen int
			if err := lg.Scan(t.Context(), snap, lokv.All(), func(r lokv.Record[int]) error {
				seen++
				if r.Revision != int64(seen) || !slices.Equal(r.Value, []int{seen, -seen}) {
					t.Fatalf("record %d: %v", seen, r)
				}
				return nil
			}); err != nil || seen != 17 {
				t.Fatalf("scan: %d, %v", seen, err)
			}
			if completions.Load() != 18 {
				t.Fatalf("got %d completions; want 17 commits and one segment", completions.Load())
			}
			assertNoUploads(t, store)
		})
	}
}

func TestMultipartPartSizing(t *testing.T) {
	store := &Store{multipartPartSize: 64 << 20}
	for _, size := range []int64{1, minPartSize, 5 << 30, 1 << 40, int64(maxParts) * maxPartSize} {
		part, err := store.partSize(size)
		if err != nil || part < minPartSize || part > maxPartSize || (size+part-1)/part > maxParts {
			t.Fatalf("size=%d part=%d: %v", size, part, err)
		}
	}
	for _, size := range []int64{int64(maxParts)*maxPartSize + 1, math.MaxInt64} {
		if _, err := store.partSize(size); err == nil {
			t.Fatalf("accepted impossible S3 object size %d", size)
		}
	}
}

func TestMultipartCancellation(t *testing.T) {
	partStarted := make(chan struct{})
	var aborts atomic.Int32
	store := fakeS3Store(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Query().Has("partNumber") {
				io.Copy(io.Discard, r.Body)
				close(partStarted)
				<-r.Context().Done()
				return
			}
			if r.Method == http.MethodDelete {
				aborts.Add(1)
			}
			next.ServeHTTP(w, r)
		})
	})
	store.multipartThreshold = 0
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- store.Create(ctx, "key", strings.NewReader("value")) }()
	select {
	case <-partStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("part request never started")
	}
	// An in-progress upload must be absent from both the object namespace and
	// the sorted listing, even though its multipart upload ID exists.
	if body, err := store.Get(t.Context(), "key"); !errors.Is(err, lokv.ErrNotFound) {
		if body != nil {
			body.Close()
		}
		t.Fatalf("incomplete object visible: %v", err)
	}
	if keys, err := store.List(t.Context(), "", 1); err != nil || len(keys) != 0 {
		t.Fatalf("incomplete upload listed: %v, %v", keys, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Create: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Create did not stop after cancellation")
	}
	if aborts.Load() != 1 {
		t.Fatalf("aborts=%d", aborts.Load())
	}
	assertNoUploads(t, store)
}

func TestMultipartAbortFailure(t *testing.T) {
	var uploads atomic.Int32
	var allowAbort atomic.Bool
	store := fakeS3Store(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Query().Has("uploads") {
				uploads.Add(1)
			}
			if isComplete(r) {
				s3Error(w, 409, "ConditionalRequestConflict")
				return
			}
			if r.Method == http.MethodDelete && !allowAbort.Load() {
				s3Error(w, 403, "AccessDenied")
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	store.multipartThreshold = 0
	err := store.Create(t.Context(), "key", strings.NewReader("value"))
	if err == nil || !strings.Contains(err.Error(), "ConditionalRequestConflict") || !strings.Contains(err.Error(), "abort multipart") || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("completion and cleanup errors not preserved: %v", err)
	}
	if uploads.Load() != 1 {
		t.Fatal("started another upload after cleanup failed")
	}
	client := store.client.(*s3.Client)
	out, err := client.ListMultipartUploads(t.Context(), &s3.ListMultipartUploadsInput{Bucket: aws.String(store.bucket)})
	if err != nil || len(out.Uploads) != 1 {
		t.Fatalf("unfinished upload: %v, %v", out, err)
	}
	allowAbort.Store(true)
	_, err = client.AbortMultipartUpload(t.Context(), &s3.AbortMultipartUploadInput{
		Bucket: aws.String(store.bucket), Key: aws.String("key"), UploadId: out.Uploads[0].UploadId,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertNoUploads(t, store)
}

func TestMultipartLostCompletionRetry(t *testing.T) {
	for _, reply := range []struct {
		status int
		code   string
	}{{404, "NoSuchUpload"}, {412, "PreconditionFailed"}} {
		t.Run(reply.code, func(t *testing.T) {
			var completions atomic.Int32
			store := fakeS3Store(t, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !isComplete(r) {
						next.ServeHTTP(w, r)
						return
					}
					if completions.Add(1) == 1 {
						result := httptest.NewRecorder()
						next.ServeHTTP(result, r)
						if result.Code != 200 {
							t.Errorf("completion failed: %s", result.Body)
						}
						// Report a retryable error after successfully publishing.
						s3Error(w, 500, "InternalError")
						return
					}
					// S3 has already consumed the upload ID; an SDK retry can
					// report its absence or the failed object precondition.
					s3Error(w, reply.status, reply.code)
				})
			})
			enableSDKRetries(store)
			store.multipartThreshold = 0
			lg, err := lokv.Open[string](lokv.Config{Store: store})
			if err != nil {
				t.Fatal(err)
			}
			record, err := lg.Append(t.Context(), "value")
			if err != nil || record.Revision != 1 || !slices.Equal(record.Value, []string{"value"}) {
				t.Fatalf("ambiguous append: %v, %v", record, err)
			}
			if completions.Load() != 2 {
				t.Fatalf("completion attempts=%d", completions.Load())
			}
			assertNoUploads(t, store)
		})
	}
}

type multipartTestClient struct {
	Client
	part     func(context.Context, *s3.UploadPartInput) (*s3.UploadPartOutput, error)
	complete func(context.Context, *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	abort    func(context.Context, *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)
}

func (c *multipartTestClient) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return c.part(ctx, in)
}

func (c *multipartTestClient) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return c.complete(ctx, in)
}

func (c *multipartTestClient) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return c.abort(ctx, in)
}

func TestMultipartWorkersAndChecksums(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail=%v", fail), func(t *testing.T) {
			store := fakeS3Store(t, nil)
			sdk := store.client
			store.multipartThreshold = 0
			store.multipartPartSize = minPartSize
			var active, calls, completes atomic.Int32
			ready := make(chan struct{})
			release := make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("part failed")
			store.client = &multipartTestClient{
				Client: sdk,
				part: func(ctx context.Context, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
					active.Add(1)
					defer active.Add(-1)
					n := calls.Add(1)
					if active.Load() > uploadConcurrency {
						t.Error("unbounded upload concurrency")
					}
					if n == uploadConcurrency {
						close(ready)
					}
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if fail {
						if aws.ToInt32(in.PartNumber) == 1 {
							return nil, failure
						}
						<-ctx.Done()
						return nil, ctx.Err()
					}
					// A virtual source verifies offsets and sizes without huge
					// objects or whole-part buffers in this scheduling test.
					var first [1]byte
					if _, err := io.ReadFull(in.Body, first[:]); err != nil || first[0] != byte(aws.ToInt32(in.PartNumber)-1) {
						t.Errorf("part %d starts at wrong offset: %v", aws.ToInt32(in.PartNumber), err)
					}
					wantLength := int64(minPartSize)
					if aws.ToInt32(in.PartNumber) == 7 {
						wantLength = 1
					}
					if aws.ToInt64(in.ContentLength) != wantLength || in.ChecksumAlgorithm != types.ChecksumAlgorithmCrc32 {
						t.Errorf("invalid part request: %v", in)
					}
					return &s3.UploadPartOutput{ETag: aws.String(fmt.Sprint(*in.PartNumber)), ChecksumCRC32: aws.String(fmt.Sprintf("crc-%d", *in.PartNumber))}, nil
				},
				complete: func(_ context.Context, in *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
					completes.Add(1)
					if active.Load() != 0 || len(in.MultipartUpload.Parts) != 7 || aws.ToString(in.IfNoneMatch) != "*" || in.ChecksumType != types.ChecksumTypeComposite {
						t.Errorf("invalid completion: %v", in)
					}
					for i, part := range in.MultipartUpload.Parts {
						if aws.ToInt32(part.PartNumber) != int32(i+1) || aws.ToString(part.ETag) != fmt.Sprint(i+1) || aws.ToString(part.ChecksumCRC32) != fmt.Sprintf("crc-%d", i+1) {
							t.Errorf("part order or checksum lost: %v", part)
						}
					}
					// The unit-test parts weren't sent to S3; discard the upload.
					_, err := sdk.AbortMultipartUpload(t.Context(), &s3.AbortMultipartUploadInput{Bucket: in.Bucket, Key: in.Key, UploadId: in.UploadId})
					return &s3.CompleteMultipartUploadOutput{}, err
				},
				abort: func(ctx context.Context, in *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
					if active.Load() != 0 || ctx.Err() != nil {
						t.Error("cleanup before workers stopped or with canceled context")
					}
					if _, ok := ctx.Deadline(); !ok {
						t.Error("cleanup without deadline")
					}
					return sdk.AbortMultipartUpload(ctx, in)
				},
			}
			done := make(chan error, 1)
			go func() { done <- store.Create(ctx, "key", virtualParts{}) }()
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("workers never filled available concurrency")
			}
			close(release)
			select {
			case err := <-done:
				if fail && !errors.Is(err, failure) || !fail && err != nil {
					t.Fatalf("Create: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("workers did not stop")
			}
			if fail && completes.Load() != 0 || !fail && completes.Load() != 1 {
				t.Fatalf("completions=%d", completes.Load())
			}
			store.client = sdk
			assertNoUploads(t, store)
		})
	}
}

type virtualParts struct{}

func (virtualParts) Size() int64 { return 6*minPartSize + 1 }

func (virtualParts) ReadAt(p []byte, offset int64) (int, error) {
	for i := range p {
		p[i] = byte((offset + int64(i)) / minPartSize)
	}
	return len(p), nil
}
