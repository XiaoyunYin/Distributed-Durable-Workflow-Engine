package state

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/partition"
)

func TestK6RegistryClass(t *testing.T) {
	definition := Definition{ActivityVersions: []byte(`{"dur053.cooperating":"v1"}`),
		EffectClasses: map[string]EffectClass{K6Activity: EffectCooperating}}
	t.Setenv("DUR053_K6_ACTIVITY", "0")
	if _, ok := RuntimeVersion(definition, K6Activity); ok {
		t.Fatal("K6 version accepted while opted out")
	}
	t.Setenv("DUR053_K6_ACTIVITY", "1")
	if version, ok := RuntimeVersion(definition, K6Activity); !ok || version != "v1" {
		t.Fatal("registered K6 version refused")
	}
	for _, drift := range []EffectClass{EffectPure, EffectNonCooperating, ""} {
		definition.EffectClasses[K6Activity] = drift
		if _, ok := RuntimeVersion(definition, K6Activity); ok {
			t.Fatal("K6 activity class drift accepted")
		}
	}
	definition.EffectClasses[K6Activity] = EffectCooperating
	definition.ActivityVersions = []byte(`{"dur053.cooperating":"v2"}`)
	if _, ok := RuntimeVersion(definition, K6Activity); ok {
		t.Fatal("unregistered K6 version accepted")
	}
}

func TestK6ProofRequiresSinkVerification(t *testing.T) {
	key, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	input := ResultInput{WorkflowID: "workflow", AttemptNumber: 2}
	attempt := k6Attempt{outcome: "OUTCOME_UNKNOWN", fence: 31}
	grant := k6Grant{K6Dispatch: K6Dispatch{IntentID: "intent", CanonicalHash: "canonical", ScopeHash: "scope",
		LogicalKey: "logical", ResourceID: "resource", ExpectedRevision: "0"}}
	claim := k6receipt.Claim{NodeID: K6Activity, Attempt: 2, Fence: 31}
	receipt := k6receipt.Receipt{WorkflowID: "workflow", IntentID: "intent", CanonicalHash: "canonical",
		ScopeHash: "scope", LogicalKey: "logical", ResourceID: "resource", AppliedRevision: 1, ApplicationID: "application"}
	proof := k6receipt.Proof{Operation: "lookup", Receipt: receipt}
	if validK6Proof(input, attempt, grant, proof, key) {
		t.Fatal("unsigned worker receipt was trusted")
	}
	proof.Signature = k6receipt.Sign(signer, claim, "lookup", receipt)
	if !validK6Proof(input, attempt, grant, proof, key) {
		t.Fatal("authenticated current lookup refused")
	}
	proof.Operation = "apply"
	proof.Signature = k6receipt.Sign(signer, claim, "apply", receipt)
	if validK6Proof(input, attempt, grant, proof, key) {
		t.Fatal("apply response cleared recorded uncertainty")
	}
	proof.Operation = "lookup"
	proof.Signature = k6receipt.Sign(signer, claim, "lookup", receipt)
	attempt.fence++
	if validK6Proof(input, attempt, grant, proof, key) {
		t.Fatal("receipt from a superseded fence was trusted")
	}
}

func TestK6DispatchKeyReuse(t *testing.T) {
	f := newK6Fixture(t, time.Minute, nil)
	definition := "k6-key-" + NewID()
	if err := f.store.CreateDefinition(f.ctx, DefinitionInput{DefinitionID: definition, Version: 1, DefinitionHash: definition,
		Graph: []byte(`{"nodes":["dur053.cooperating"]}`), ActivityVersions: []byte(`{"dur053.cooperating":"v1"}`),
		EffectClasses: []byte(`{"dur053.cooperating":"COOPERATING_EFFECT"}`)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		workflow := NewID()
		for mapped, _ := partition.ID(workflow); int16(mapped) != f.lease.PartitionID; mapped, _ = partition.ID(workflow) {
			workflow = NewID()
		}
		if _, err := f.store.CreateWorkflow(f.ctx, CreateWorkflowInput{WorkflowID: workflow, Namespace: k6TestNamespace,
			SubmissionKey: workflow, SubmissionPayloadHash: workflow, DefinitionID: definition, DefinitionVersion: 1,
			PartitionID: f.lease.PartitionID, InitialNodeID: K6Activity, InitialInput: []byte(`{}`), ActorID: "client"}); err != nil {
			t.Fatal(err)
		}
		input := f.input(workflow, 1)
		input.NodeID, input.K6LogicalKey, input.Target = K6Activity, "shared-key", "resource-"+workflow
		_, err := f.store.CreateApprovalIntent(f.ctx, input)
		if i == 0 && err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			if !errors.Is(err, ErrEffectIdentityMismatch) {
				t.Fatal("second logical operation reused the first key")
			}
			wf, err := f.store.GetWorkflow(f.ctx, workflow)
			if err != nil || wf.State != StateRunnable || wf.Revision != 1 {
				t.Fatal("key conflict changed workflow state")
			}
			intents, err := f.store.ListApprovalIntents(f.ctx, workflow)
			if err != nil || len(intents) != 0 {
				t.Fatal("key conflict left an approval intent")
			}
		}
	}
}
