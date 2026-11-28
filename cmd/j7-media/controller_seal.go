package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const candidatePath = "deploy/j7/candidate.json"

func candidateDigests(root string) (map[string]string, error) {
	source, err := evidenceFile(root, relayReaderPath)
	if err != nil || !strings.Contains(string(source), repairedRelaySwitch) {
		return nil, errors.New("J9A_ADMISSION: reviewed relay reader is unavailable")
	}
	if _, err := readControllerPlan(root); err != nil {
		return nil, err
	}
	expected := append(slices.Clone(hostedCandidateFiles), relayReaderPath)
	digests := make(map[string]string, len(expected))
	for _, p := range expected {
		if err := checkedEvidenceDir(filepath.Dir(filepath.Join(root, p))); err != nil {
			return nil, err
		}
		digest, err := fileSHA(root, p)
		if err != nil {
			return nil, fmt.Errorf("J7_CANDIDATE_ADMISSION: cannot seal %s: %w", p, err)
		}
		digests[p] = digest
	}
	return digests, nil
}

func resealCandidate(root string, out io.Writer) error {
	if err := checkedEvidenceDir(root); err != nil {
		return err
	}
	name, _, err := readRelayAdmission(root)
	if err != nil {
		return err
	}
	digests, err := candidateDigests(root)
	if err != nil {
		return err
	}
	c := hostedCandidate{Schema: "j7.hosted-candidate.v1", RelaySwitch: name, RelayValue: "1", RuntimePath: relayReaderPath, Files: digests}
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(root, candidatePath)
	if _, err := evidenceFile(root, candidatePath); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".candidate-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		return errors.Join(err, tmp.Close())
	}
	if err := errors.Join(tmp.Sync(), tmp.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, candidatePath)
	return err
}
