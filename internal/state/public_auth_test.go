package state

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

type tenantApprovalTx struct {
	pgx.Tx
	row  pgx.Row
	args []any
}

func (tx *tenantApprovalTx) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	tx.args = args
	return tx.row
}

type tenantApprovalRow struct {
	workflowID, issuer, requester string
	err                           error
}

func (row tenantApprovalRow) Scan(dest ...any) error {
	if row.err == nil {
		*dest[0].(*string), *dest[1].(*string), *dest[2].(*string) = row.workflowID, row.issuer, row.requester
	}
	return row.err
}

func TestJ12TenantDecisionRequiresDistinctBoundRequester(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  tenantApprovalRow
		want error
	}{
		{"distinct", tenantApprovalRow{workflowID: "workflow", issuer: "issuer", requester: "requester"}, nil},
		{"self", tenantApprovalRow{workflowID: "workflow", issuer: "issuer", requester: "approver"}, ErrApprovalUnauthorized},
		{"legacy", tenantApprovalRow{workflowID: "workflow"}, ErrApprovalUnauthorized},
		{"changed-issuer", tenantApprovalRow{workflowID: "workflow", issuer: "old-issuer", requester: "requester"}, ErrApprovalUnauthorized},
		{"outside-scope", tenantApprovalRow{err: pgx.ErrNoRows}, ErrApprovalNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &tenantApprovalTx{row: tc.row}
			workflow, err := lockTenantApprovalWorkflow(t.Context(), tx, "path-intent", TenantPrincipal{
				Issuer: "issuer", Subject: "approver", Namespace: "tenant-a",
			})
			if len(tx.args) != 2 || tx.args[0] != "path-intent" || tx.args[1] != "tenant-a" {
				t.Fatalf("approval did not query the stored path intent in the authenticated namespace: %v", tx.args)
			}
			if !errors.Is(err, tc.want) || (tc.want == nil && workflow != "workflow") || (tc.want != nil && workflow != "") {
				t.Fatalf("approval workflow=%q error=%v, want %v", workflow, err, tc.want)
			}
		})
	}
}
