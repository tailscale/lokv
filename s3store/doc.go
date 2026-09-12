// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

// Package s3store adapts general-purpose Amazon S3 buckets to [lokv.Store].
// Every PUT uses If-None-Match: *. Directory buckets and eventually consistent
// or unordered S3-compatible services do not satisfy the lokv storage contract.
//
// # IAM and bucket configuration
//
// For a log whose normalized lokv.Config.Prefix is PREFIX, grant writers these
// S3 permissions:
//   - s3:ListBucket on arn:aws:s3:::BUCKET, with a StringLike condition restricting
//     s3:prefix to PREFIX/v1/*.
//   - s3:GetObject and s3:PutObject on arn:aws:s3:::BUCKET/PREFIX/v1/*.
//
// Read-only clients need the same listing and read permissions, without
// s3:PutObject. For an empty log prefix, omit PREFIX/ from both paths above.
// These are the S3 permissions; encryption with a customer-managed KMS key also
// requires the corresponding KMS permissions in IAM and the key policy.
//
// Add explicit bucket-policy denies for s3:DeleteObject and
// s3:DeleteObjectVersion on the log's objects, and for object-creation PUTs
// missing If-None-Match. The [example bucket policy] supplies these denies;
// replace BUCKET and PREFIX for the deployment. It contains no permission grants,
// so the allow permissions above must be supplied separately. Keep application
// credentials unable to change the bucket policy or lifecycle configuration.
//
// Disable lifecycle expiration for the log's prefix. Prefer the Bucket owner
// enforced setting for object ownership. Versioning alone does not prevent
// replacement of the current object. Object Lock in compliance mode can add
// protection against administrators, but does not replace conditional creation
// or the delete deny. [New] validates configuration without I/O and cannot verify
// these deployment properties.
//
// [example bucket policy]: https://github.com/tailscale/lokv/blob/main/examples/immutable-bucket-policy.json
package s3store
