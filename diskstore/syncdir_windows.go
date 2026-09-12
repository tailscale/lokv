// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package diskstore

// Windows does not support syncing directory handles with os.File.Sync.
func syncDir(string) error { return nil }
