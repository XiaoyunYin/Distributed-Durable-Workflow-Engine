//go:build !dur034_ablation

package main

import "context"

func campaignProbeContext(ctx context.Context) context.Context { return ctx }

func campaignOutboxSuppressionEnabled() bool { return false }
