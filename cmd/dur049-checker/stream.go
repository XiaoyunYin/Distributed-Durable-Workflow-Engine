package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"

	"durable-agent-execution-engine/internal/invariants"
)

func writeReport(output io.Writer, result report) error {
	w := reportStream{output: bufio.NewWriterSize(output, 64*1024)}
	w.encoder = json.NewEncoder(&w)
	w.value(reflect.ValueOf(result))
	w.text("\n")
	if w.err != nil {
		return w.err
	}
	return w.output.Flush()
}

type reportStream struct {
	output  *bufio.Writer
	encoder *json.Encoder
	err     error
}

// Encoder handles each leaf's escaping and JSON representation. Its trailing
// newline is only needed at the end of the whole report.
func (w *reportStream) Write(p []byte) (int, error) {
	n, err := w.output.Write(p[:len(p)-1])
	if err != nil {
		return n, err
	}
	return n + 1, nil
}

func (w *reportStream) text(s string) {
	if w.err == nil {
		_, w.err = w.output.WriteString(s)
	}
}

func (w *reportStream) leaf(v any) {
	if w.err == nil {
		w.err = w.encoder.Encode(v)
	}
}

// Walk only report containers. Delegate the rows, scalars and small objects
// to encoding/json, including RawMessage and its legacy HTML escaping.
func (w *reportStream) value(v reflect.Value) {
	if w.err != nil {
		return
	}
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}
	switch v.Interface().(type) {
	case report, invariants.Trace, invariants.SnapshotTable, invariants.CampaignReport:
		w.text("{")
		first := true
		for i := range v.NumField() {
			name, omitEmpty := jsonField(v.Type().Field(i))
			field := v.Field(i)
			if omitEmpty && emptyJSONField(field) {
				continue
			}
			if !first {
				w.text(",")
			}
			first = false
			w.leaf(name)
			w.text(":")
			w.value(field)
		}
		w.text("}")
		return
	case map[string]invariants.SnapshotTable:
		if !v.IsNil() {
			w.text("{")
			keys := v.MapKeys()
			sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
			for i, key := range keys {
				if i != 0 {
					w.text(",")
				}
				w.leaf(key.String())
				w.text(":")
				w.value(v.MapIndex(key))
			}
			w.text("}")
			return
		}
	}
	if v.Kind() == reflect.Slice && !v.IsNil() && v.Type().Elem().Kind() != reflect.Uint8 {
		w.text("[")
		for i := 0; i < v.Len() && w.err == nil; i++ {
			if i != 0 {
				w.text(",")
			}
			w.leaf(v.Index(i).Interface())
		}
		w.text("]")
		return
	}
	w.leaf(v.Interface())
}

func jsonField(field reflect.StructField) (name string, omitEmpty bool) {
	name, option, _ := strings.Cut(field.Tag.Get("json"), ",")
	if name == "" {
		name = field.Name
	}
	return name, option == "omitempty"
}

func emptyJSONField(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Map, reflect.Slice:
		return v.Len() == 0
	default:
		return v.IsZero()
	}
}

func readSnapshot(path string) (invariants.Trace, error) {
	file, err := os.Open(path)
	if err != nil {
		return invariants.Trace{}, err
	}
	defer file.Close()
	return decodeSnapshot(file)
}

func decodeSnapshot(input io.Reader) (invariants.Trace, error) {
	d := snapshotDecoder{json.NewDecoder(input)}
	d.UseNumber()
	var trace invariants.Trace
	if err := d.container(reflect.ValueOf(&trace).Elem()); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return invariants.Trace{}, errors.New("unexpected end of JSON input")
		}
		return invariants.Trace{}, err
	}
	if _, err := d.Token(); err != io.EOF {
		if err == nil {
			err = errors.New("unexpected data after snapshot")
		}
		return invariants.Trace{}, err
	}
	return trace, nil
}

type snapshotDecoder struct{ *json.Decoder }

// The only containers in Trace are the top-level object, the snapshot table
// map, each table object, and slices of rows. Read each row as a normal JSON
// value so Decoder never buffers an entire known section.
func (d snapshotDecoder) container(dest reflect.Value) error {
	opening, err := d.Token()
	if err != nil {
		return err
	}
	if opening == nil {
		if dest.Kind() != reflect.Struct {
			dest.SetZero()
		}
		return nil
	}
	want := json.Delim('{')
	if dest.Kind() == reflect.Slice {
		want = '['
	}
	if opening != want {
		return fmt.Errorf("unexpected JSON token %v for %s", opening, dest.Type())
	}
	if dest.Kind() == reflect.Slice {
		err = d.rows(dest)
	} else {
		err = d.object(dest)
	}
	if err != nil {
		return err
	}
	_, err = d.Token()
	return err
}

func (d snapshotDecoder) rows(dest reflect.Value) error {
	rows := dest
	if !d.More() {
		dest.Set(reflect.MakeSlice(dest.Type(), 0, 0))
		return nil
	}
	count := 0
	for d.More() {
		if count == rows.Cap() {
			rows = reflect.Append(rows, reflect.Zero(rows.Type().Elem()))
		} else if count == rows.Len() {
			rows = rows.Slice(0, count+1)
		}
		if err := d.Decode(rows.Index(count).Addr().Interface()); err != nil {
			return err
		}
		count++
	}
	// The standard decoder truncates repeated array fields and clears any
	// abandoned elements so they no longer hold row references.
	rows.Slice(count, rows.Len()).Clear()
	dest.Set(rows.Slice(0, count))
	return nil
}

func (d snapshotDecoder) object(dest reflect.Value) error {
	if dest.Kind() == reflect.Map && dest.IsNil() {
		dest.Set(reflect.MakeMap(dest.Type()))
	}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		name := key.(string) // Token enforces object key syntax.
		if dest.Kind() == reflect.Map {
			table := reflect.New(dest.Type().Elem()).Elem()
			if err := d.container(table); err != nil {
				return err
			}
			dest.SetMapIndex(reflect.ValueOf(name), table)
			continue
		}
		field := snapshotField(dest, name)
		if !field.IsValid() {
			err = d.skipValue()
		} else if field.Kind() == reflect.Slice || field.Kind() == reflect.Map {
			err = d.container(field)
		} else {
			err = d.Decode(field.Addr().Interface())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func snapshotField(dest reflect.Value, name string) reflect.Value {
	for i := range dest.NumField() {
		field, _ := jsonField(dest.Type().Field(i))
		if strings.EqualFold(field, name) {
			return dest.Field(i)
		}
	}
	return reflect.Value{}
}

func (d snapshotDecoder) skipValue() error {
	depth := 0
	for {
		token, err := d.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'), json.Delim('['):
			depth++
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
		if depth <= 0 {
			return nil
		}
	}
}
