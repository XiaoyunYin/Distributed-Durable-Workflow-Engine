// Package migrations exposes the unchanged top-level SQL migrations to the K3 Job.
package migrations

import "embed"

//go:embed *.up.sql
var SQL embed.FS
