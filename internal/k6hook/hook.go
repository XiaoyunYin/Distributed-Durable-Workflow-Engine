// Package k6hook defines the campaign boundary contract. The default build
// provides a nil client, even when supplied campaign configuration.
package k6hook

import "time"

type Identity struct {
	Actor   string `json:"actor"`
	UID     string `json:"uid"`
	Process string `json:"process_start_id"`
	Session string `json:"session"`
}

type Fields struct {
	Key        string            `json:"key"`
	Invocation string            `json:"invocation"`
	Request    string            `json:"request"`
	Intent     string            `json:"intent"`
	Resource   string            `json:"resource"`
	Scope      string            `json:"scope"`
	Binding    string            `json:"binding"`
	Reply      string            `json:"reply"`
	App        string            `json:"application"`
	Objects    map[string]string `json:"objects,omitempty"`
}

type Client interface {
	Hit(boundary string, fields Fields) (Hold, error)
}

type Hold interface {
	Action() string
	Deadline() time.Time
	Finish() error
}
