package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"durable-agent-execution-engine/internal/k6receipt"
	"github.com/jackc/pgx/v5"
)

func provisionFixtureReceipt(t *testing.T, engine, sink *pgx.Conn, m *k6v1.Manifest, workflow string) k6receipt.Proof {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	m.ReceiptPublicKey = base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey))
	receipt := k6receipt.Receipt{WorkflowID: workflow, LogicalKey: "key"}
	if err := sink.QueryRow(context.Background(), `SELECT resource_id,canonical_hash,scope_hash,intent_id,expected_revision,applied_revision,application_id
		FROM k6_sink.receipts WHERE workflow_id=$1 AND logical_key=$2`, workflow, receipt.LogicalKey).
		Scan(&receipt.ResourceID, &receipt.CanonicalHash, &receipt.ScopeHash, &receipt.IntentID, &receipt.ExpectedRevision,
			&receipt.AppliedRevision, &receipt.ApplicationID); err != nil {
		t.Fatal(err)
	}
	claim := k6receipt.Claim{NodeID: "pure.echo", Attempt: 1, Fence: 1}
	proof := k6receipt.Proof{Operation: "apply", Receipt: receipt, Signature: k6receipt.Sign(key, claim, "apply", receipt)}
	raw, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	execK6SQL(t, engine, `INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class,logical_effect_key,k6_fence,k6_proof)
		VALUES ($1,'pure.echo',0,1,'SUCCEEDED','COOPERATING_EFFECT','key',1,$2)
		ON CONFLICT (workflow_id,node_id,iteration,attempt_number) DO UPDATE SET k6_fence=1,k6_proof=EXCLUDED.k6_proof`,
		workflow, raw)
	return proof
}

func requirePredicateFinding(t *testing.T, result k6v1.Result, kind k6v1.FindingKind, predicate, reason string) {
	t.Helper()
	for _, finding := range result.Findings {
		if finding.Kind == kind && finding.Predicate == predicate && finding.Code == reason {
			return
		}
	}
	t.Fatalf("wanted %s %s %s, got %+v", kind, predicate, reason, result.Findings)
}

func requireMinimalHistoryProof(t *testing.T, result k6v1.Result) {
	t.Helper()
	findings := databaseFindings(result)
	if len(findings) != 1 {
		t.Fatalf("minimal history findings: %+v", findings)
	}
	requirePredicateFinding(t, result, k6v1.MissingProof, "AtomicHistory", "required_proof_missing:engine/engine.k4_audit.jsonl")
}

func verifyVerdictParity(t *testing.T, path string, stdout []byte, code int) k6v1.Result {
	t.Helper()
	bundle := inspectK6Output(t, path, stdout, code)
	if root := os.Getenv("DURABLE_K6_VERDICT_ARTIFACT"); root != "" {
		dir := filepath.Join(root, filepath.FromSlash(t.Name()))
		public := filepath.Join(dir, "public")
		for name, member := range bundle {
			writeVerdictArtifact(t, filepath.Join(public, filepath.FromSlash(name)), member.Data)
		}
		checker := os.Getenv("DURABLE_K6_VERDICT_CHECKER")
		cmd := exec.Command("sudo", "unshare", "--net", "--", checker, k6Offline, public)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		offline, err := cmd.Output()
		writeVerdictArtifact(t, filepath.Join(dir, "live.json"), stdout)
		writeVerdictArtifact(t, filepath.Join(dir, "offline.json"), offline)
		writeVerdictArtifact(t, filepath.Join(dir, "offline.stderr"), stderr.Bytes())
		if cmd.ProcessState == nil {
			t.Fatalf("start network-isolated checker: %v", err)
		}
		exit := cmd.ProcessState.ExitCode()
		writeVerdictArtifact(t, filepath.Join(dir, "exit.json"), []byte(`{"live":`+strconv.Itoa(code)+`,"offline":`+strconv.Itoa(exit)+"}\n"))
		if exit != code || !bytes.Equal(stdout, offline) || stderr.Len() != 0 {
			t.Fatalf("network-isolated replay differs: exit %d, want %d: %v %s", exit, code, err, &stderr)
		}
	}
	result := k6v1.Verify(bundle)
	for _, finding := range result.Findings {
		ref := finding.Witness
		switch finding.Predicate {
		case "AtomicHistory", "OwnerAtCommit", "CurrentClaimWinner", "DispatchDisposition",
			"LowerOffsetsDisposed", "LostResultReplay", "UnknownEffectVisible", "ShutdownDisposition":
		case "RevisionChain":
			if finding.Code != "SINK_RESET_RESOURCE_MISSING" {
				continue
			}
		default:
			continue
		}
		if ref == nil {
			continue
		}
		member := bundle[ref.Path]
		if member == nil {
			t.Fatalf("witness path %s is absent from the public bundle", ref.Path)
		}
		if ref.Path == k6v1.ManifestPath && k6v1.Hash(member.Data) == ref.SHA256 {
			continue
		}
		if !slices.ContainsFunc(bytes.SplitAfter(member.Data, []byte{'\n'}), func(line []byte) bool {
			return len(line) > 0 && k6v1.Hash(line) == ref.SHA256
		}) {
			t.Fatalf("witness %s %v does not hash to a public row", ref.Path, ref.RowKey)
		}
	}
	return result
}

func writeVerdictArtifact(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestK6SinkVerdictParity(t *testing.T) {
	engine, sink, cfg, plan, arrivals := pairedK6Fixture(t)
	workflow := plan.Windows[0].Submissions[0].Key
	proof := provisionFixtureReceipt(t, engine, sink, &cfg.Manifest, workflow)
	verify := func(t *testing.T, config k6live.Config) k6v1.Result {
		t.Helper()
		config.EpisodeDir = filepath.Join(t.TempDir(), "public")
		config = writeK6Sources(t, config, plan, arrivals)
		path, out, diag, code := invokeLiveK6(t, config)
		if diag != "" {
			t.Fatal(diag)
		}
		return verifyVerdictParity(t, path, out, code)
	}
	t.Run("Clean", func(t *testing.T) {
		requireMinimalHistoryProof(t, verify(t, cfg))
	})
	t.Run("ResetResourceMissing", func(t *testing.T) {
		mutant := cfg
		mutant.Manifest.SinkResetResources = []k6v1.ResourceBaseline{{ResourceID: "missing-resource", StateHash: k6v1.Hash([]byte("{}"))}}
		r := verify(t, mutant)
		requireOrderingFinding(t, r, "RevisionChain", "SINK_RESET_RESOURCE_MISSING", k6v1.Violation,
			k6v1.ManifestPath, []any{"sink_reset_resources", "missing-resource"})
		if label := r.Labels[7]; label.Outcome != "covered/fail" || label.Reason != "contract_violation" {
			t.Fatalf("RevisionChain label: %+v", label)
		}
	})
	t.Run("WrongReceiptKey", func(t *testing.T) {
		mutant := cfg
		key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, ed25519.SeedSize))
		mutant.Manifest.ReceiptPublicKey = base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey))
		requirePredicateFinding(t, verify(t, mutant), k6v1.Violation, "ReceiptSignature", "RECEIPT_SIGNATURE_INVALID")
	})
	t.Run("MissingReceiptKey", func(t *testing.T) {
		mutant := cfg
		mutant.Manifest.ReceiptPublicKey = ""
		requirePredicateFinding(t, verify(t, mutant), k6v1.MissingProof, "ReceiptSignature", "required_proof_missing:receipt_public_key")
	})
	setProof := func(t *testing.T, transform string) {
		t.Helper()
		raw, err := json.Marshal(proof)
		if err != nil {
			t.Fatal(err)
		}
		execK6SQL(t, engine, "UPDATE engine.activity_attempts SET k6_proof="+transform, string(raw))
	}
	for _, tc := range []struct {
		name, transform, reason string
		kind                    k6v1.FindingKind
	}{
		{"MissingSignature", "$1::jsonb - 'signature'", "required_proof_missing:ReceiptSignature", k6v1.MissingProof},
		{"ChangedReceiptByte", `jsonb_set($1::jsonb,'{receipt,resource_id}','"resourcf"')`, "RECEIPT_SIGNATURE_INVALID", k6v1.Violation},
		{"MissingReceipt", "NULLIF($1::jsonb,$1::jsonb)", "required_proof_missing:ReceiptSignature", k6v1.MissingProof},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setProof(t, tc.transform)
			t.Cleanup(func() { setProof(t, "$1::jsonb") })
			requirePredicateFinding(t, verify(t, cfg), tc.kind, "ReceiptSignature", tc.reason)
		})
	}
	t.Run("NonCanonicalReceipt", func(t *testing.T) {
		raw, err := json.Marshal(struct {
			Claim     k6receipt.Claim
			Operation string
			Receipt   k6receipt.Receipt
		}{k6receipt.Claim{NodeID: "pure.echo", Attempt: 1, Fence: 1}, proof.Operation, proof.Receipt})
		if err != nil {
			t.Fatal(err)
		}
		key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
		nonCanonical := append(append([]byte("dur053-k6-sink-receipt/v1\x00"), raw...), '\n')
		signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, nonCanonical))
		execK6SQL(t, engine, `UPDATE engine.activity_attempts SET k6_proof=jsonb_set(k6_proof,'{signature}',to_jsonb($1::text))`, signature)
		t.Cleanup(func() { setProof(t, "$1::jsonb") })
		requirePredicateFinding(t, verify(t, cfg), k6v1.Violation, "ReceiptSignature", "RECEIPT_SIGNATURE_INVALID")
	})
	t.Run("DuplicateMutation", func(t *testing.T) {
		execK6SQL(t, sink, `UPDATE k6_sink.resources SET revision=2, state='{"value":"k6","position":1}' WHERE resource_id='resource'`)
		t.Cleanup(func() {
			execK6SQL(t, sink, `ALTER TABLE k6_sink.resources DISABLE TRIGGER journal_resource_write;
				UPDATE k6_sink.resources SET revision=1, state='{"value":"k6"}' WHERE resource_id='resource';
				DELETE FROM k6_sink.journal WHERE resource_id='resource' AND revision=2;
				ALTER TABLE k6_sink.resources ENABLE TRIGGER journal_resource_write`)
		})
		requirePredicateFinding(t, verify(t, cfg), k6v1.Violation, "OneMutationPerKey", "SINK_KEY_MUTATED_TWICE")
	})
	t.Run("Preseeded", func(t *testing.T) {
		execK6SQL(t, sink, `UPDATE k6_sink.journal SET old_state_hash=repeat('e',64) WHERE resource_id='resource'`)
		t.Cleanup(func() {
			execK6SQL(t, sink, `UPDATE k6_sink.journal SET old_state_hash=$1 WHERE resource_id='resource'`, k6v1.Hash([]byte("{}")))
		})
		requirePredicateFinding(t, verify(t, cfg), k6v1.Violation, "RevisionChain", "SINK_INITIAL_ENDPOINT")
	})
}

func TestK6SourceTerminalVerdictParity(t *testing.T) {
	engine, dsn := k6TestDatabase(t)
	bindK6Database(t, engine, "engine-instance", "reset-a")
	m := k6Manifest(engine.Config().Database, false)
	cfg := k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn, Manifest: m}
	plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "pure.echo")
	execK6SQL(t, engine, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}')`)
	minimalExecution(t, engine, plan.Windows[0].Submissions[0])
	verify := func(t *testing.T) k6v1.Result {
		t.Helper()
		cfg.EpisodeDir = filepath.Join(t.TempDir(), "public")
		cfg = writeK6Sources(t, cfg, plan, arrivals)
		path, out, diag, code := invokeLiveK6(t, cfg)
		if diag != "" {
			t.Fatal(diag)
		}
		return verifyVerdictParity(t, path, out, code)
	}
	t.Run("Clean", func(t *testing.T) {
		if label := verify(t).Labels[10]; label.Outcome != "covered/pass" {
			t.Fatalf("terminal verdict: %+v", label)
		}
	})
	t.Run("SecondTerminal", func(t *testing.T) {
		execK6SQL(t, engine, `UPDATE engine.transition_history SET new_state='SUCCEEDED' WHERE revision=1`)
		t.Cleanup(func() {
			execK6SQL(t, engine, `UPDATE engine.transition_history SET new_state='RUNNABLE' WHERE revision=1`)
		})
		requirePredicateFinding(t, verify(t), k6v1.Violation, "UniqueTerminal", "TERMINAL_FINAL_UNIQUE")
	})
	t.Run("MissingTerminal", func(t *testing.T) {
		execK6SQL(t, engine, `DELETE FROM engine.transition_history WHERE revision=2`)
		requirePredicateFinding(t, verify(t), k6v1.Violation, "UniqueTerminal", "TERMINAL_FINAL_UNIQUE")
	})
}
