//go:build !dur053_k6

package state

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func configureK6Capture(*pgxpool.Config) {}

func captureOwner(context.Context, pgx.Tx, LeaseRef) (bool, error) { return false, nil }

func captureResult(context.Context, pgx.Tx, ResultInput, string, string) error { return nil }

func captureDelivery(context.Context, pgx.Tx, InboxMessage, InboxDisposition) error { return nil }

func (*Store) CapturePublicationStart(context.Context, OutboxEvent) error { return nil }
