//go:build !dur034_ablation

package state

import "context"

// The production build has no DUR-034 profile type or implementation. These
// no-op helpers keep the normal Store code's call sites safe and compile-time
// inert; the actual weakened profile code exists only under dur034_ablation.
func applyTestOwnerTransition(*Store, context.Context, OwnerTransitionInput) (error, bool) {
	return nil, false
}

func testSafeguardCheck(context.Context) error { return nil }

func suppressHistory(context.Context) bool { return false }

func suppressOutbox(context.Context) bool { return false }
