//go:build !dur053_k6

package k6hook

func FromEnvironment(string, string) (Client, error) { return nil, nil }
