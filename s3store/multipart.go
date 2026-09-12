// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/tailscale/lokv"
)

const (
	minPartSize       = 5 << 20
	maxPartSize       = 5 << 30
	maxParts          = 10_000
	uploadConcurrency = 4
	abortTimeout      = 30 * time.Second
)

// partSize accommodates the entire object within S3's part count and size
// limits. These are backend limits, not additional compaction admission limits.
func (s *Store) partSize(size int64) (int64, error) {
	if size > int64(maxParts)*maxPartSize {
		return 0, errors.New("object exceeds S3's multipart capacity")
	}
	return max(minPartSize, s.multipartPartSize, (size+maxParts-1)/maxParts), nil
}

func (s *Store) createMultipart(ctx context.Context, key string, value lokv.SizeReaderAt, size int64, contentType string) error {
	partSize, err := s.partSize(size)
	if err != nil {
		return s.wrap("multipart", key, err)
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		retry, err := s.multipartAttempt(ctx, key, value, size, partSize, contentType)
		if !retry || attempt >= s.retries {
			return err
		}
		if err := waitConflict(ctx, attempt); err != nil {
			return err
		}
	}
}

// multipartAttempt owns one upload ID. Only a completion conflict permits
// starting a new upload; ambiguous errors must reach lokv's readback resolution.
func (s *Store) multipartAttempt(ctx context.Context, key string, value io.ReaderAt, size, partSize int64, contentType string) (retry bool, err error) {
	out, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), ContentType: aws.String(contentType),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32, ChecksumType: types.ChecksumTypeComposite,
	})
	if err != nil {
		return false, s.wrap("initiate multipart", key, err)
	}
	if out == nil || aws.ToString(out.UploadId) == "" {
		return false, s.wrap("initiate multipart", key, lokv.ErrCorrupt)
	}
	defer func() {
		if err == nil {
			return
		}
		// Workers have stopped using value before cleanup starts. Cancellation
		// of the operation must not prevent cleanup of its unfinished upload.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
		defer cancel()
		_, abortErr := s.client.AbortMultipartUpload(cleanupCtx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: out.UploadId,
		})
		// Completion might have succeeded but its response was lost.
		if abortErr != nil && code(abortErr) != "NoSuchUpload" {
			err = errors.Join(err, s.wrap("abort multipart", key, abortErr))
			retry = false
		}
	}()
	parts, err := s.uploadParts(ctx, key, out.UploadId, value, size, partSize)
	if err != nil {
		return false, err
	}
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: out.UploadId,
		IfNoneMatch:     aws.String("*"),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		ChecksumType:    types.ChecksumTypeComposite,
	})
	if err == nil {
		return false, nil
	}
	if status(err) == 412 || code(err) == "PreconditionFailed" {
		return false, s.wrap("complete multipart", key, fmt.Errorf("%w: %w", lokv.ErrExists, err))
	}
	return status(err) == 409 || code(err) == "ConditionalRequestConflict", s.wrap("complete multipart", key, err)
}

func (s *Store) uploadParts(ctx context.Context, key string, uploadID *string, value io.ReaderAt, size, partSize int64) ([]types.CompletedPart, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	parts := make([]types.CompletedPart, (size+partSize-1)/partSize)
	var wg sync.WaitGroup
	for worker := 0; worker < min(uploadConcurrency, len(parts)); worker++ {
		wg.Go(func() {
			for i := worker; i < len(parts); i += uploadConcurrency {
				if ctx.Err() != nil {
					return
				}
				offset := int64(i) * partSize
				length := min(partSize, size-offset)
				out, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: uploadID,
					PartNumber: aws.Int32(int32(i + 1)),
					Body:       io.NewSectionReader(value, offset, length), ContentLength: aws.Int64(length),
					ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
				})
				if err == nil && (out == nil || aws.ToString(out.ETag) == "") {
					err = lokv.ErrCorrupt
				}
				if err != nil {
					cancel(s.wrap(fmt.Sprintf("upload part %d", i+1), key, err))
					return
				}
				parts[i] = types.CompletedPart{
					PartNumber: aws.Int32(int32(i + 1)), ETag: out.ETag, ChecksumCRC32: out.ChecksumCRC32,
				}
			}
		})
	}
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return parts, nil
}
