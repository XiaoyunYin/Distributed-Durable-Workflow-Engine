package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
)

const evidenceLimit = 64 << 20

var evidenceNames = strings.Fields(`
  workload.json topology.json policy.json marker-plan.json ack.jsonl
  backup.json backup-manifest.json source-binding.json cut.json source-loss.json
  cohorts.json source-proof.json restore.json engine-pre-write.json sink-pre-write.json
  kafka-source.json kafka-fresh.json consumers.json redispatch-watch.json redispatch.json
  endpoint.json quiet-start.json quiet-end.json engine-quiet-start.json engine-final.json sink-quiet-start.json
  owner.json worker.json
  j7-source-owner.json j7-owner-expiry.json j7-owner-before.json j7-owner-after-denial.json
  j7-owner-committed.json j7-owner-released.json j7-owner-terminal.json
  j7-source-claim.json j7-worker-replacement.json j7-worker-before.json j7-worker-after-denial.json j7-worker-terminal.json
  orphan.json source-engine.json source-sink.json reader-rebind.json final-engine.json final-sink.json effect-ingress.jsonl
  resource-samples.json qualification-controls.json cleanup.json verdict.json
`)

type evidenceEntry struct {
	Name     string `json:"name"`
	Bytes    int64  `json:"bytes"`
	SHA      string `json:"sha256"`
	Original string `json:"original_sha256,omitempty"`
}

type evidenceIndex struct {
	Schema     string          `json:"schema"`
	Revision   string          `json:"revision"`
	Campaign   string          `json:"campaign"`
	Drill      string          `json:"drill"`
	Run        uint64          `json:"run_id"`
	Attempt    uint32          `json:"run_attempt"`
	Projection bool            `json:"redacted_projection"`
	Files      []evidenceEntry `json:"files"`
	Missing    []string        `json:"missing"`
	Withheld   []string        `json:"withheld,omitempty"`
}

type evidenceBundle struct {
	dir   string
	index evidenceIndex
	files map[string]evidenceEntry
}

func evidenceFile(dir, name string) ([]byte, error) {
	f, err := openRegular(filepath.Join(dir, name), os.O_RDONLY)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, evidenceLimit+1))
	if err != nil || len(data) > evidenceLimit {
		return nil, fmt.Errorf("evidence %s is unreadable or oversized", name)
	}
	return data, nil
}

func checkedEvidenceDir(dir string) error {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return errors.New("evidence directory must be an absolute canonical path")
	}
	real, err := filepath.EvalSymlinks(dir)
	if err != nil || real != dir {
		return errors.New("evidence directory must resolve without symlinks")
	}
	return nil
}

func openBundle(dir, expected string) (evidenceBundle, error) {
	b := evidenceBundle{dir: dir, files: map[string]evidenceEntry{}}
	if err := checkedEvidenceDir(dir); err != nil {
		return b, err
	}
	data, err := evidenceFile(dir, "index.json")
	if err != nil {
		return b, err
	}
	if !restoreledger.IsSHA256(expected) || restoreledger.SHA256(data) != expected {
		return b, errors.New("evidence index differs from the independently supplied digest")
	}
	if err := evidenceJSON(data, &b.index); err != nil {
		return b, err
	}
	if b.index.Schema != "j7.evidence-index.v1" || len(b.index.Revision) != 40 || b.index.Run == 0 ||
		b.index.Attempt == 0 || !fixtureName.MatchString(b.index.Campaign) || !fixtureName.MatchString(b.index.Drill) {
		return b, errors.New("index has no candidate, run or drill binding")
	}
	for _, e := range b.index.Files {
		if !slices.Contains(evidenceNames, e.Name) || e.Bytes <= 0 || e.Bytes > evidenceLimit ||
			!restoreledger.IsSHA256(e.SHA) || (e.Original != "" &&
			(!b.index.Projection || !isEngineTrace(e.Name) || !restoreledger.IsSHA256(e.Original))) ||
			b.files[e.Name].Name != "" {
			return b, errors.New("invalid, duplicate or unsafe evidence index entry")
		}
		b.files[e.Name] = e
		if _, err := b.read(e.Name); err != nil {
			return b, err
		}
	}
	missing := missingEvidence(b.files)
	if !slices.Equal(missing, b.index.Missing) {
		return b, errors.New("evidence index does not list the exact missing artifacts")
	}
	for i, name := range b.index.Withheld {
		if !b.index.Projection || !slices.Contains(missing, name) || slices.Contains(b.index.Withheld[:i], name) {
			return b, errors.New("unaccounted withheld evidence")
		}
	}
	return b, nil
}

func missingEvidence(files map[string]evidenceEntry) []string {
	missing := []string{}
	for _, name := range evidenceNames {
		if files[name].Name == "" {
			missing = append(missing, name)
		}
	}
	return missing
}

func (b evidenceBundle) read(name string) ([]byte, error) {
	e, exists := b.files[name]
	if !exists {
		return nil, fmt.Errorf("missing indexed evidence %s", name)
	}
	data, err := evidenceFile(b.dir, name)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != e.Bytes || restoreledger.SHA256(data) != e.SHA {
		return nil, fmt.Errorf("indexed digest or length differs for %s", name)
	}
	return data, nil
}

func (b evidenceBundle) original(name string) string {
	e := b.files[name]
	if e.Original != "" {
		return e.Original
	}
	return e.SHA
}

func evidenceJSON(raw []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("evidence requires one complete JSON value")
	}
	return nil
}

func (b evidenceBundle) decode(name string, v any) error {
	raw, err := b.read(name)
	if err != nil {
		return err
	}
	if err := evidenceJSON(raw, v); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func isEngineTrace(name string) bool {
	return name == "engine-pre-write.json" || name == "engine-quiet-start.json" || name == "engine-final.json"
}

const tokenPrefix = "j7sha256:"

func credentialDigest(s string) string {
	if hexDigest, ok := strings.CutPrefix(s, tokenPrefix); ok && restoreledger.IsSHA256(hexDigest) {
		return hexDigest
	}
	return restoreledger.SHA256([]byte(s))
}

func fingerprintToken(token string) string {
	if token != "" {
		return tokenPrefix + restoreledger.SHA256([]byte(token))
	}
	return ""
}

func redactTrace(raw []byte) ([]byte, error) {
	var t invariants.Trace
	if err := evidenceJSON(raw, &t); err != nil {
		return nil, err
	}
	if err := invariants.SnapshotIntegrity(t); err != nil {
		return nil, err
	}
	for i := range t.Attempts {
		t.Attempts[i].ClaimToken = fingerprintToken(t.Attempts[i].ClaimToken)
	}
	for i := range t.Approvals {
		t.Approvals[i].GrantToken = fingerprintToken(t.Approvals[i].GrantToken)
	}
	for name, table := range t.Snapshot {
		for i, row := range table.Rows {
			var value any
			d := json.NewDecoder(bytes.NewReader(row))
			d.UseNumber()
			if err := d.Decode(&value); err != nil {
				return nil, err
			}
			if redactTokens(value) {
				safe, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				table.Rows[i] = safe
			}
		}
		canonical, err := json.Marshal(table.Rows)
		if err != nil {
			return nil, err
		}
		table.SHA256 = restoreledger.SHA256(canonical)
		t.Snapshot[name] = table
	}
	if err := invariants.SnapshotIntegrity(t); err != nil {
		return nil, err
	}
	return json.Marshal(t)
}

func redactTokens(v any) bool {
	changed := false
	switch value := v.(type) {
	case map[string]any:
		for key, item := range value {
			switch strings.ToLower(key) {
			case "claim_token", "grant_token":
				if token, ok := item.(string); ok {
					value[key] = fingerprintToken(token)
					changed = true
				}
			default:
				changed = redactTokens(item) || changed
			}
		}
	case []any:
		for _, item := range value {
			changed = redactTokens(item) || changed
		}
	}
	return changed
}

func fileSHA(dir, name string) (string, error) {
	f, err := openRegular(filepath.Join(dir, name), os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func captureIndex(dir string, idx evidenceIndex) (evidenceIndex, string, error) {
	idx.Schema = "j7.evidence-index.v1"
	entries := map[string]evidenceEntry{}
	for _, name := range evidenceNames {
		data, err := evidenceFile(dir, name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return idx, "", err
		}
		if len(data) == 0 {
			continue // an interrupted exclusive receipt publication is incomplete
		}
		e := evidenceEntry{Name: name, Bytes: int64(len(data)), SHA: restoreledger.SHA256(data)}
		entries[name] = e
		idx.Files = append(idx.Files, e)
	}
	idx.Missing = missingEvidence(entries)
	sha, err := savePrivateJSON(filepath.Join(dir, "index.json"), idx)
	return idx, sha, err
}

func publishBundle(src evidenceBundle, dest string) (string, error) {
	if src.index.Projection || !filepath.IsAbs(dest) || filepath.Clean(dest) != dest {
		return "", errors.New("publishing requires a raw source and new absolute evidence directory")
	}
	if err := checkedEvidenceDir(filepath.Dir(dest)); err != nil {
		return "", err
	}
	if err := os.Mkdir(dest, 0o700); err != nil {
		return "", err
	}
	idx := src.index
	idx.Projection = true
	idx.Files = nil
	kept := map[string]evidenceEntry{}
	for _, e := range src.index.Files {
		data, err := publicEvidence(src, e)
		if err != nil {
			idx.Withheld = append(idx.Withheld, e.Name)
			continue
		}
		if isEngineTrace(e.Name) {
			e.Original = e.SHA
			e.SHA, e.Bytes = restoreledger.SHA256(data), int64(len(data))
		}
		// Publish only the fixed evidence inventory. Deployment settings in
		// topology/consumers are the redacted records, never the live overlays.
		if err := writeEvidenceBytes(filepath.Join(dest, e.Name), data); err != nil {
			return "", err
		}
		idx.Files = append(idx.Files, e)
		kept[e.Name] = e
	}
	idx.Missing = missingEvidence(kept)
	return savePrivateJSON(filepath.Join(dest, "index.json"), idx)
}

func publicEvidence(src evidenceBundle, e evidenceEntry) ([]byte, error) {
	data, err := src.read(e.Name)
	if err != nil {
		return nil, err
	}
	if isEngineTrace(e.Name) {
		data, err = redactTrace(data)
		if err != nil {
			return nil, err
		}
	}
	for _, line := range evidenceDocuments(e.Name, data) {
		var value any
		if !json.Valid(line) || json.Unmarshal(line, &value) != nil || !safeEvidenceValue(value) {
			return nil, fmt.Errorf("cannot publish %s: malformed evidence or credential-bearing field", e.Name)
		}
	}
	return data, nil
}

func evidenceDocuments(name string, data []byte) [][]byte {
	if strings.HasSuffix(name, ".jsonl") {
		return bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	}
	return [][]byte{data}
}

func safeEvidenceValue(v any) bool {
	switch value := v.(type) {
	case map[string]any:
		if name, ok := value["name"].(string); ok {
			if _, hasValue := value["value"]; hasValue &&
				name != "RUNTIME_KAFKA_GENERATION" && name != "KAFKA_BOOTSTRAP_SERVERS" {
				return false
			}
			if _, indirect := value["valueFrom"]; indirect {
				return false
			}
		}
		for key, item := range value {
			field := strings.ToLower(strings.ReplaceAll(key, "_", ""))
			switch field {
			case "authorization", "password", "privatekey", "dsn", "grantreaderdsn", "sinkdsn", "secret":
				if item != nil && item != "" {
					return false
				}
			}
			if strings.HasSuffix(field, "token") && item != nil {
				s, ok := item.(string)
				if !ok || (s != "" && (!strings.HasPrefix(s, tokenPrefix) || !restoreledger.IsSHA256(strings.TrimPrefix(s, tokenPrefix)))) {
					return false
				}
			}
			if !safeEvidenceValue(item) {
				return false
			}
		}
	case []any:
		for _, item := range value {
			if !safeEvidenceValue(item) {
				return false
			}
		}
	case string:
		if strings.Contains(value, "-----BEGIN PRIVATE KEY-----") ||
			strings.Contains(value, "-----BEGIN RSA PRIVATE KEY-----") ||
			strings.Contains(value, "password=") ||
			((strings.HasPrefix(value, "postgres://") || strings.HasPrefix(value, "postgresql://")) && strings.Contains(value, "@")) ||
			strings.Contains(value, "Bearer ") {
			return false
		}
	}
	return true
}

func writeEvidenceBytes(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	return errors.Join(writeErr, f.Sync(), f.Close(), syncDirectory(filepath.Dir(path)))
}

func runEvidence(args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	dir := flags.String("dir", "", "canonical evidence directory")
	dest := flags.String("destination", "", "new directory for redacted evidence")
	sha := flags.String("index-sha256", "", "digest of the evidence index")
	head := flags.String("revision", "", "exact candidate revision")
	campaign := flags.String("campaign", "", "campaign identifier")
	drill := flags.String("drill", "", "drill identifier")
	run := flags.Uint64("run", 0, "GitHub run ID")
	attempt := flags.Uint("attempt", 0, "GitHub run attempt")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("evidence commands accept flags only")
	}
	if err := checkedEvidenceDir(*dir); err != nil {
		return err
	}
	switch args[0] {
	case "index-evidence":
		if !fixtureName.MatchString(*campaign) || !fixtureName.MatchString(*drill) || *run == 0 ||
			*attempt == 0 || *attempt > 1<<32-1 || len(*head) != 40 {
			return errors.New("evidence index requires exact campaign, drill, run, attempt and candidate")
		}
		if _, err := hex.DecodeString(*head); err != nil {
			return errors.New("candidate must be an exact hexadecimal commit")
		}
		_, digest, err := captureIndex(*dir, evidenceIndex{
			Revision: *head, Campaign: *campaign, Drill: *drill, Run: *run, Attempt: uint32(*attempt),
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]string{"evidence_index_sha256": digest})
	case "publish-evidence":
		b, err := openBundle(*dir, *sha)
		if err != nil {
			return err
		}
		// Withhold unchecked or credential-bearing members and record exactly
		// what was withheld. The remaining partial evidence can still be retained.
		digest, err := publishBundle(b, *dest)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]string{"evidence_index_sha256": digest})
	}
	return errors.New("unknown evidence command")
}
