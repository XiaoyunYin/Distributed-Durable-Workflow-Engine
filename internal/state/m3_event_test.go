package state

import "testing"

func TestEventDefinitionsUseExplicitTopics(t *testing.T) {
	wantTasks := map[string]bool{
		"workflow.created":         false,
		"attempt.dispatch":         true,
		"attempt.redispatch":       true,
		"activity.result":          false,
		"workflow.result_consumed": false,
		"graph.advanced":           false,
		"timer.scheduled":          false,
		"workflow.canceled":        false,
		"reconciliation.required":  false,
		"reconciliation.resolved":  false,
	}
	for eventType, taskTopic := range wantTasks {
		t.Run(eventType, func(t *testing.T) {
			topic, ok := EventTopicFor(eventType)
			if !ok {
				t.Fatal("event type is missing from the shared registry")
			}
			want := EventTopic
			if taskTopic {
				want = TaskTopic
			}
			if topic != want {
				t.Fatalf("topic = %q, want %q", topic, want)
			}
		})
	}
	if got := NormalizeResultEventType(""); got != DefaultResultEventType {
		t.Fatalf("empty result event type = %q, want %q", got, DefaultResultEventType)
	}
	if _, ok := EventTopicFor("unregistered.event"); ok {
		t.Fatal("unregistered event type was accepted")
	}
}
