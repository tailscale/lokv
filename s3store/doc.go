// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package s3store adapts general-purpose Amazon S3 buckets to [lokv.Store].
// Object publication uses If-None-Match: * with PutObject or multipart upload
// completion. Directory buckets and eventually consistent or unordered
// S3-compatible services do not satisfy the lokv storage contract.
//
// Large values are uploaded as streaming parts; callers still supply a single
// [lokv.SizeReaderAt]. Multipart changes the transfer, not the stored bytes or
// the number of objects and GETs needed to read the log.
//
// # IAM and bucket configuration
//
// For a log whose normalized lokv.Config.Prefix is PREFIX, grant writers these
// S3 permissions:
//   - s3:ListBucket on arn:aws:s3:::BUCKET, with a StringLike condition restricting
//     s3:prefix to PREFIX/v1/*.
//   - s3:GetObject and s3:PutObject on arn:aws:s3:::BUCKET/PREFIX/v1/*.
//   - s3:AbortMultipartUpload on the same object resource, to discard unfinished
//     uploads after failures. This does not permit deleting completed objects.
//
// Read-only clients need the same listing and read permissions, without
// s3:PutObject or s3:AbortMultipartUpload. For an empty log prefix, omit PREFIX/
// from both paths above.
// These are the S3 permissions; encryption with a customer-managed KMS key also
// requires the corresponding KMS permissions in IAM and the key policy.
//
// Add explicit bucket-policy denies for s3:DeleteObject and
// s3:DeleteObjectVersion on the log's objects, and for object creation missing
// If-None-Match. The [example bucket policy] supplies these denies;
// replace BUCKET and PREFIX for the deployment. It contains no permission grants,
// so the allow permissions above must be supplied separately. Its
// s3:ObjectCreationOperation condition exempts multipart initiation and part
// uploads, which do not publish an object and cannot carry If-None-Match.
// Keep application credentials unable to change the bucket policy or lifecycle
// configuration.
//
// Disable lifecycle expiration for the log's prefix. Configure a lifecycle rule
// with AbortIncompleteMultipartUpload to reclaim uploads stranded by process
// crashes, lost initiation responses, or failed aborts. Choose DaysAfterInitiation
// to exceed the longest expected upload (for example, seven days). This rule
// removes unfinished parts without expiring completed log objects. The adapter
// attempts cleanup itself but cannot guarantee it after a crash or network error.
//
// Prefer the Bucket owner enforced setting for object ownership. Versioning
// alone does not prevent replacement of the current object. Object Lock in
// compliance mode can add protection against administrators, but does not
// replace conditional creation or the delete deny. [New] validates configuration
// without I/O and cannot verify these deployment properties.
//
// [example bucket policy]: https://github.com/tailscale/lokv/blob/main/examples/immutable-bucket-policy.json
package s3store
