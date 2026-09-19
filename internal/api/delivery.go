package api

import (
	"errors"
	"net/http"
	"os"

	"durable-agent-execution-engine/internal/state"
)

// The local API persists inbox/poison disposition before the Python consumer
// commits Kafka offsets. Payload is base64 so malformed JSON can be quarantined.
func (s *Server) workerDelivery(w http.ResponseWriter, r *http.Request) {
	store, ok := s.repository.(*state.Store)
	if !ok {
		writeError(w, 501, "WORKER_CONTROL_UNAVAILABLE", "durable delivery store is unavailable", nil)
		return
	}
	var request struct {
		EventID       string `json:"event_id"`
		Topic         string `json:"topic"`
		Partition     int    `json:"partition"`
		Offset        int64  `json:"offset"`
		EventType     string `json:"event_type"`
		SchemaVersion int    `json:"schema_version"`
		Payload       []byte `json:"payload"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, 400, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	if request.Topic != state.TaskTopic || request.Partition < 0 || request.Offset < 0 {
		writeError(w, 400, "INVALID_REQUEST", "a task-topic delivery with a nonnegative offset is required", nil)
		return
	}
	message := state.InboxMessage{ConsumerID: s.workerConsumerID, EventID: request.EventID,
		Topic: request.Topic, Partition: request.Partition, Offset: request.Offset, EventType: request.EventType,
		SchemaVersion: request.SchemaVersion, Payload: request.Payload}
	result, err := store.RecordInbox(r.Context(), message)
	if errors.Is(err, state.ErrInvalidEvent) || errors.Is(err, state.ErrOutboxNotFound) || errors.Is(err, state.ErrEventConflict) {
		result, err = store.QuarantineMessage(r.Context(), message, err.Error())
	}
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	var task *state.RuntimeActivity
	if result.Disposition != state.InboxQuarantined {
		namespace := os.Getenv("RUNTIME_NAMESPACE")
		if namespace == "" {
			namespace = "local-runtime"
		}
		task, err = store.RuntimeActivity(r.Context(), namespace, request.Payload)
		if err != nil {
			writeRepositoryError(w, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"disposition": result.Disposition, "commit_offset": result.CommitOffset,
		"next_offset": result.NextOffset, "task": task})
}
