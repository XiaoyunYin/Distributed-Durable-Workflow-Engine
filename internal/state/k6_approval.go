package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

var ErrK6ArgumentsNotCanonical = errors.New("K6 approval arguments are not canonical")

// K6ExistingIntent identifies the logical operation's non-replaceable intent.
type K6ExistingIntent struct{ IntentID string }

func (*K6ExistingIntent) Error() string { return "K6 approval intent already exists" }

// K6AlreadyGranted identifies an intent whose original grant remains in force
// until the provisioned cutoff.
type K6AlreadyGranted struct{ IntentID string }

func (*K6AlreadyGranted) Error() string { return "K6 approval was already granted" }

// ValidateK6Arguments checks the sealed-plan JSON spelling before hashing it.
func ValidateK6Arguments(arguments json.RawMessage) error {
	if !utf8.Valid(arguments) || !json.Valid(arguments) {
		return ErrK6ArgumentsNotCanonical
	}
	decoder := json.NewDecoder(bytes.NewReader(arguments))
	decoder.UseNumber()
	return validateK6Value(decoder)
}

func validateK6Value(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return ErrK6ArgumentsNotCanonical
	}
	switch value := token.(type) {
	case json.Number:
		integer, err := value.Int64()
		const maxSafeInteger = 1<<53 - 1
		if err != nil || value.String() == "-0" || integer < -maxSafeInteger || integer > maxSafeInteger {
			return ErrK6ArgumentsNotCanonical
		}
	case json.Delim:
		var keys map[string]struct{}
		if value == '{' {
			keys = make(map[string]struct{})
		}
		for decoder.More() {
			if keys != nil {
				key, err := decoder.Token()
				if err != nil {
					return ErrK6ArgumentsNotCanonical
				}
				name := key.(string) // json.Valid and the opening object delimiter guarantee a key.
				if _, duplicate := keys[name]; duplicate {
					return ErrK6ArgumentsNotCanonical
				}
				keys[name] = struct{}{}
			}
			if err := validateK6Value(decoder); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return ErrK6ArgumentsNotCanonical
		}
	}
	return nil
}

type k6ApprovalWindow struct {
	namespace string
	cutoff    time.Time
	latest    time.Time // cutoff less the bounded DB clock-probe uncertainty
	lockBy    time.Time // DB remaining time expressed in Go's monotonic clock
}

// Set the wait budget using DB time before taking the lease/workflow locks.
// Once expired, allow an uncontended lookup of the original intent identity.
func loadK6ApprovalWindow(ctx context.Context, tx pgx.Tx) (k6ApprovalWindow, error) {
	var window k6ApprovalWindow
	var sampled time.Time
	started := time.Now()
	if err := tx.QueryRow(ctx, `SELECT namespace, valid_until,
		valid_until - uncertainty_us * interval '1 microsecond', clock_timestamp()
		FROM k6_auth.episode`).Scan(&window.namespace, &window.cutoff, &window.latest, &sampled); err != nil {
		return window, err
	}
	window.lockBy = started.Add(window.latest.Sub(sampled))
	waitMillis := max(int64(1), time.Until(window.lockBy).Milliseconds())
	// PostgreSQL lock_timeout's integer range is milliseconds.
	waitMillis = min(waitMillis, 1<<31-1)
	_, err := tx.Exec(ctx, `SELECT pg_catalog.set_config('lock_timeout',
		LEAST($1::integer, COALESCE(NULLIF(setting::integer, 0), $1))::text, true)
		FROM pg_catalog.pg_settings WHERE name = 'lock_timeout'`, int32(waitMillis))
	return window, err
}

func (w k6ApprovalWindow) lockContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if time.Until(w.lockBy) <= 0 {
		// A request after cutoff may identify its original intent under the
		// lease, using the 1 ms server lock limit and this bounded query budget.
		return context.WithTimeout(ctx, 100*time.Millisecond)
	}
	return context.WithDeadline(ctx, w.lockBy)
}

func (w k6ApprovalWindow) check(ctx context.Context, tx pgx.Tx) error {
	var sampled time.Time
	if err := tx.QueryRow(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&sampled); err != nil {
		return err
	}
	if !sampled.Before(w.latest) {
		return ErrApprovalExpired
	}
	return nil
}

func (w k6ApprovalWindow) authorizeNode(ctx context.Context, tx pgx.Tx, workflow Workflow, node string) error {
	if workflow.Namespace != w.namespace {
		return ErrApprovalMismatch
	}
	var effectClass *string
	if err := tx.QueryRow(ctx, `SELECT effect_classes ->> $3::text FROM engine.workflow_definitions
		WHERE definition_id = $1 AND version = $2`, workflow.DefinitionID, workflow.DefinitionVersion, node).Scan(&effectClass); err != nil {
		return err
	}
	if effectClass == nil || EffectClass(*effectClass) != EffectCooperating {
		return ErrEffectClassMismatch
	}
	return nil
}

func verifyK6StoredArguments(ctx context.Context, tx pgx.Tx, input ApprovalIntentInput, intentID, hash string) error {
	var stored json.RawMessage
	if err := tx.QueryRow(ctx, `SELECT canonical_arguments FROM engine.approval_action_intents WHERE intent_id = $1`,
		intentID).Scan(&stored); err != nil {
		return err
	}
	storedHash, err := proposalHash(input.Target, stored, input.ExpectedResourceRevision)
	if err != nil || storedHash != hash {
		return ErrK6ArgumentsNotCanonical
	}
	return nil
}
