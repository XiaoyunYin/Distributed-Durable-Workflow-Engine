// Package smokeplan describes the sealed engineering fixture shared by its
// lease holder and scripted approver.
package smokeplan

import "encoding/json"

type Plan struct {
	Namespace, WorkflowID, NodeID, Target, ResourceRevision, Key string
	Arguments                                                    json.RawMessage
}
