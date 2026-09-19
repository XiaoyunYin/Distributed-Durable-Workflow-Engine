// Package incident contains the production PostgreSQL incident workflow
// integration. It deliberately uses the durable Go engine and effects service;
// the M6 SQLite adapter is not part of this package.
package incident

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"durable-agent-execution-engine/internal/effects"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
)

const (
	DefinitionID      = "dur033a-incident-remediation"
	DefinitionVersion = 1
)

// SourceHit is evidence returned by the production source_corpus boundary.
type SourceHit struct {
	ChunkID    string
	DocumentID string
	Version    string
	Service    string
	Text       string
}

// SourceFixture identifies rows seeded for one bounded integration run.
type SourceFixture struct {
	DocumentID string
	ChunkID    string
	CaseID     string
}

// SourceCorpus is a small production PostgreSQL reader for the schema created
// by migration 000014. It is intentionally separate from engine.workflow_*.
type SourceCorpus struct {
	store *state.Store
}

func NewSourceCorpus(store *state.Store) *SourceCorpus {
	return &SourceCorpus{store: store}
}

// SeedFixture writes one deterministic document, chunk, and fixture case into
// the declared source boundary. IDs are caller-scoped so cleanup cannot touch
// another run's evidence.
func (s *SourceCorpus) SeedFixture(ctx context.Context, runID string) (SourceFixture, error) {
	if s == nil || s.store == nil || runID == "" {
		return SourceFixture{}, errors.New("source corpus store and run ID are required")
	}
	fixture := SourceFixture{
		DocumentID: "dur033a-doc-" + runID,
		ChunkID:    "dur033a-chunk-" + runID,
		CaseID:     "dur033a-case-" + runID,
	}
	tx, err := s.store.Pool().Begin(ctx)
	if err != nil {
		return SourceFixture{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO source_corpus.documents
			(document_id, version, source_type, service, title, source_text, corpus_version)
		VALUES ($1, 'v1', 'runbook', 'checkout', 'Connection pool recovery',
			$2, 'dur033a-v1')`, fixture.DocumentID,
		"Checkout connection pool saturation is remediated by restoring the pool replicas."); err != nil {
		return SourceFixture{}, fmt.Errorf("seed source document: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO source_corpus.chunks
			(chunk_id, document_id, version, ordinal, chunk_text, embedding_json, embedding_model, corpus_version)
		VALUES ($1, $2, 'v1', 0, $3, '[]'::jsonb, 'durable-hash-embed-v1', 'dur033a-v1')`,
		fixture.ChunkID, fixture.DocumentID,
		"If checkout shows connection pool saturation, restore the pool to the approved replica count before verifying service health."); err != nil {
		return SourceFixture{}, fmt.Errorf("seed source chunk: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO source_corpus.fixture_cases
			(case_id, split, family, service, answerable, document_dependent, expected_diagnosis, expected_action)
		VALUES ($1, 'development', 'connection_pool', 'checkout', true, true,
			'connection_pool_saturation', $2::jsonb)`, fixture.CaseID,
		`{"action":"restore","replicas":1}`); err != nil {
		return SourceFixture{}, fmt.Errorf("seed source case: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SourceFixture{}, fmt.Errorf("commit source fixture: %w", err)
	}
	return fixture, nil
}

func (s *SourceCorpus) Search(ctx context.Context, query string) ([]SourceHit, error) {
	if s == nil || s.store == nil || query == "" {
		return nil, errors.New("source corpus store and query are required")
	}
	rows, err := s.store.Pool().Query(ctx, `
		SELECT c.chunk_id, c.document_id, c.version, d.service, c.chunk_text
		FROM source_corpus.chunks AS c
		JOIN source_corpus.documents AS d
		  ON d.document_id = c.document_id AND d.version = c.version
		WHERE c.search_vector @@ plainto_tsquery('simple', $1)
		ORDER BY ts_rank_cd(c.search_vector, plainto_tsquery('simple', $1)) DESC,
			c.chunk_id`, query)
	if err != nil {
		return nil, fmt.Errorf("query production source corpus: %w", err)
	}
	defer rows.Close()
	var hits []SourceHit
	for rows.Next() {
		var hit SourceHit
		if err := rows.Scan(&hit.ChunkID, &hit.DocumentID, &hit.Version, &hit.Service, &hit.Text); err != nil {
			return nil, fmt.Errorf("scan source hit: %w", err)
		}
		hits = append(hits, hit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source hits: %w", err)
	}
	return hits, nil
}

func (s *SourceCorpus) DeleteFixture(ctx context.Context, fixture SourceFixture) error {
	if s == nil || s.store == nil {
		return errors.New("source corpus store is required")
	}
	if _, err := s.store.Pool().Exec(ctx, `DELETE FROM source_corpus.fixture_cases WHERE case_id = $1`, fixture.CaseID); err != nil {
		return err
	}
	_, err := s.store.Pool().Exec(ctx, `DELETE FROM source_corpus.documents WHERE document_id = $1`, fixture.DocumentID)
	return err
}

// InstallDefinition registers the immutable versioned graph used by the
// production integration. The scheduler creates the approval intent after
// the investigation node is durably complete and holds the partition lease.
func InstallDefinition(ctx context.Context, store *state.Store, definitionID string) error {
	if definitionID == "" {
		definitionID = DefinitionID
	}
	return store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: DefinitionVersion,
		DefinitionHash: "sha256:dur033a-production-v1",
		Graph: json.RawMessage(`{"entry":"investigate","nodes":[
			{"id":"investigate","kind":"activity","next":"remediation"},
			{"id":"remediation","kind":"activity"}
		]}`),
		ActivityVersions: json.RawMessage(`{"investigate":"dur033a-investigation-v1","remediation":"dur033a-remediation-v1"}`),
		EffectClasses:    json.RawMessage(`{"investigate":"PURE_ACTIVITY","remediation":"COOPERATING_EFFECT"}`),
	})
}

type investigationPayload struct {
	Diagnosis                string          `json:"diagnosis"`
	Citations                []string        `json:"citations"`
	ActionType               string          `json:"action_type"`
	TargetResourceID         string          `json:"target_resource_id"`
	CanonicalArguments       json.RawMessage `json:"canonical_arguments"`
	ExpectedResourceRevision string          `json:"expected_resource_revision"`
}

// InvestigationDriver is executed by the real engine scheduler. Its only
// evidence source is the PostgreSQL source_corpus query, and its result is the
// durable proposal consumed by the scheduler-owned approval step.
type InvestigationDriver struct {
	Corpus                   *SourceCorpus
	Query                    string
	Diagnosis                string
	TargetResourceID         string
	CanonicalArguments       json.RawMessage
	ExpectedResourceRevision string
}

func (d InvestigationDriver) Run(ctx context.Context, _ engine.Activity) (engine.ActivityResult, error) {
	hits, err := d.Corpus.Search(ctx, d.Query)
	if err != nil {
		return engine.ActivityResult{}, err
	}
	if len(hits) == 0 {
		return engine.ActivityResult{}, errors.New("investigation found no source-corpus evidence")
	}
	citations := make([]string, 0, len(hits))
	for _, hit := range hits {
		citations = append(citations, hit.ChunkID)
	}
	arguments := d.CanonicalArguments
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{"action":"restore","replicas":1}`)
	}
	payload, err := json.Marshal(investigationPayload{
		Diagnosis: d.Diagnosis, Citations: citations, ActionType: "restore",
		TargetResourceID: d.TargetResourceID, CanonicalArguments: arguments,
		ExpectedResourceRevision: d.ExpectedResourceRevision,
	})
	if err != nil {
		return engine.ActivityResult{}, err
	}
	return engine.ActivityResult{AttemptState: state.AttemptSucceeded, Payload: payload}, nil
}

// EffectDriver is the production engine activity driver for the approved
// remediation. The effect service independently recomputes the argument hash,
// checks the approval resource/revision/grant, and returns its durable receipt.
type EffectDriver struct {
	Service          *effects.Service
	IntentID         string
	GrantToken       string
	GrantScopeHash   string
	TargetResourceID string
	ExpectedRevision string
}

func (d EffectDriver) Run(ctx context.Context, activity engine.Activity) (engine.ActivityResult, error) {
	var proposal investigationPayload
	if err := json.Unmarshal(activity.Input, &proposal); err != nil {
		return engine.ActivityResult{}, fmt.Errorf("decode remediation proposal: %w", err)
	}
	if proposal.TargetResourceID != d.TargetResourceID {
		return engine.ActivityResult{}, errors.New("remediation proposal target changed")
	}
	argumentHash, err := state.CanonicalPayloadHash(proposal.CanonicalArguments)
	if err != nil {
		return engine.ActivityResult{}, err
	}
	receipt, err := d.Service.Apply(ctx, state.EffectApplyInput{
		WorkflowID: activity.WorkflowID, NodeID: activity.NodeID, Iteration: activity.Iteration,
		LogicalEffectKey: activity.LogicalEffectKey, ArgumentHash: argumentHash,
		AttemptNumber: activity.AttemptNumber, GrantScopeHash: d.GrantScopeHash,
		ExpectedResourceRevision: d.ExpectedRevision,
		RequestID:                "dur033a:" + activity.WorkflowID + ":" + activity.LogicalEffectKey,
		ResourceID:               d.TargetResourceID, FenceToken: 1, State: proposal.CanonicalArguments,
		IntentID: d.IntentID, GrantToken: d.GrantToken,
	})
	if err != nil {
		return engine.ActivityResult{}, err
	}
	return engine.ActivityResult{AttemptState: state.AttemptSucceeded, Payload: receipt.Receipt}, nil
}

// ArgumentHash is used by the committed attack matrix so each negative
// control submits a hash derived from the state it actually sends.
func ArgumentHash(arguments json.RawMessage) (string, error) {
	return state.CanonicalPayloadHash(arguments)
}
