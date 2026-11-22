package k6ingress

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestK6IngressTokenProjection(t *testing.T) {
	const sentinel = "eyJLeVNlY3JldA.nested-sentinel.signature"
	body := fmt.Sprintf(`{"intent_id":"00000000-0000-0000-0000-000000000001","attempt":1,"grant_token":%q,"arguments":{"token":%q},"logical_key":%q}`,
		sentinel, sentinel, sentinel)
	wire := fmt.Sprintf("POST /v1/apply HTTP/1.1\r\nHost: relay\r\nContent-Length: %d\r\nAuthorization: Bearer %s\r\n\r\n%s", len(body), sentinel, body)
	request, err := readRequest(bufio.NewReader(bytes.NewBufferString(wire)))
	if err != nil {
		t.Fatal(err)
	}
	r := inventory(request, 1)
	raw, err := json.Marshal(r)
	if err != nil || bytes.Contains(raw, []byte(sentinel)) || r.GrantHash != Hash([]byte(sentinel)) ||
		r.BodyHash != Hash([]byte(body)) || r.WireHash != Hash([]byte(wire)) {
		t.Fatal("ingress credential projection failed")
	}
}
