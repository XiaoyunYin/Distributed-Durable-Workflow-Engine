// Package faults provides the reusable named-boundary client used by fault
// fixtures and, later, by runtime/worker failpoint instrumentation.
package faults

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
)

// Client is nil when fault control is not enabled. Callers can safely skip a
// boundary in normal runs by checking the return from FromEnvironment.
type Client struct {
	connection net.Conn
	encoder    *json.Encoder
	decoder    *json.Decoder
	token      string
	seed       int
}

type message struct {
	Event    string         `json:"event,omitempty"`
	Command  string         `json:"command,omitempty"`
	Boundary string         `json:"boundary"`
	Fields   map[string]any `json:"fields,omitempty"`
	Token    string         `json:"token"`
	Seed     int            `json:"seed"`
}

// FromEnvironment connects to a controller when DURABLE_FAULT_ENDPOINT is
// present and otherwise returns nil. The endpoint is host:port.
func FromEnvironment() (*Client, error) {
	endpoint := os.Getenv("DURABLE_FAULT_ENDPOINT")
	if endpoint == "" {
		return nil, nil
	}
	token := os.Getenv("DURABLE_FAULT_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("DURABLE_FAULT_TOKEN is required with DURABLE_FAULT_ENDPOINT")
	}
	seedText := os.Getenv("DURABLE_FAULT_SEED")
	seed, err := strconv.Atoi(seedText)
	if err != nil {
		return nil, fmt.Errorf("invalid DURABLE_FAULT_SEED: %w", err)
	}
	connection, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("connect fault controller: %w", err)
	}
	return &Client{
		connection: connection,
		encoder:    json.NewEncoder(connection),
		decoder:    json.NewDecoder(bufio.NewReader(connection)),
		token:      token,
		seed:       seed,
	}, nil
}

// Hit reports a named boundary and blocks until the controller releases it.
func (c *Client) Hit(boundary string, fields map[string]any) error {
	if err := c.encoder.Encode(message{
		Event:    "boundary_reached",
		Boundary: boundary,
		Fields:   fields,
		Token:    c.token,
		Seed:     c.seed,
	}); err != nil {
		return fmt.Errorf("report fault boundary: %w", err)
	}
	var command message
	if err := c.decoder.Decode(&command); err != nil {
		return fmt.Errorf("read fault command: %w", err)
	}
	if command.Token != c.token || command.Command != "release" {
		return fmt.Errorf("unexpected fault command %q", command.Command)
	}
	return nil
}

// Emit records a protocol event after a boundary command, such as released.
func (c *Client) Emit(event, boundary string, fields map[string]any) error {
	return c.encoder.Encode(message{
		Event:    event,
		Boundary: boundary,
		Fields:   fields,
		Token:    c.token,
		Seed:     c.seed,
	})
}

// Close closes the controller connection.
func (c *Client) Close() error {
	return c.connection.Close()
}
