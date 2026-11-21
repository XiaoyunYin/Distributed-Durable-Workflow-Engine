package k6v1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const TimeLayout = "2006-01-02T15:04:05.000000000Z"

var (
	errJSON     = errors.New("K6_INVALID_JSON")
	integer     = regexp.MustCompile(`^(0|-?[1-9][0-9]*)$`)
	digest      = regexp.MustCompile(`^[a-f0-9]{64}$`)
	uuid        = regexp.MustCompile(`^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$`)
	identity    = regexp.MustCompile(`^[\pL\pN\pM_.:@/+<>&=-]{0,512}$`)
	traceparent = regexp.MustCompile(`^([a-f0-9]{2}-[a-f0-9]{32}-[a-f0-9]{16}-[a-f0-9]{2})?$`)
	jwt         = regexp.MustCompile(`^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$`)
)

func publicIdentity(s string) bool {
	return identity.MatchString(s) && !strings.Contains(s, "://") && !jwt.MatchString(s)
}

func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func UTC(t time.Time) string { return t.UTC().Format(TimeLayout) }

func validTime(s string) bool {
	t, err := time.Parse(TimeLayout, s)
	return err == nil && UTC(t) == s
}

// Canonical emits sorted keys at every level, even for structs.
func Canonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, errJSON
	}
	value, err := parse(raw)
	if err != nil {
		return nil, err
	}
	return encode(value)
}

func encode(value any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, errJSON
	}
	return buf.Bytes(), nil
}

// DecodeStrict enforces exact field names, presence, and shape, including
// nested fields. Errors never copy a key or value from untrusted input.
func DecodeStrict(data []byte, dst any) error {
	value, err := parse(data)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(dst) != nil {
		return errJSON
	}
	canonical, err := Canonical(dst)
	input, inputErr := encode(value)
	if err != nil || inputErr != nil || !bytes.Equal(input, canonical) {
		return errJSON
	}
	return nil
}

func decodeCanonical(data []byte, dst any) error {
	if err := DecodeStrict(data, dst); err != nil {
		return err
	}
	canonical, err := Canonical(dst)
	if err != nil || !bytes.Equal(data, canonical) {
		return errJSON
	}
	return nil
}

func parse(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, errJSON
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	value, err := parseValue(dec, 0)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errJSON
	}
	return value, nil
}

func parseValue(dec *json.Decoder, depth int) (any, error) {
	if depth > 32 {
		return nil, errJSON
	}
	token, err := dec.Token()
	if err != nil {
		return nil, errJSON
	}
	switch token {
	case json.Delim('{'):
		obj := make(map[string]any)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, errJSON
			}
			name, ok := key.(string)
			if !ok {
				return nil, errJSON
			}
			if _, duplicate := obj[name]; duplicate {
				return nil, errJSON
			}
			value, err := parseValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			obj[name] = value
		}
		return closeValue(dec, '}', obj)
	case json.Delim('['):
		items := []any{}
		for dec.More() {
			item, err := parseValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return closeValue(dec, ']', items)
	}
	if n, ok := token.(json.Number); ok {
		if !integer.MatchString(n.String()) {
			return nil, errJSON
		}
		if _, err := strconv.ParseInt(n.String(), 10, 64); err != nil {
			return nil, errJSON
		}
	}
	return token, nil
}

func closeValue(dec *json.Decoder, want json.Delim, value any) (any, error) {
	token, err := dec.Token()
	if err != nil || token != want {
		return nil, errJSON
	}
	return value, nil
}
