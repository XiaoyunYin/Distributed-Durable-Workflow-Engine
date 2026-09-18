package effects

import (
	"context"
	"errors"
	"testing"
)

func TestNonCooperatingEndpointMakesAmbiguityVisibleToCaller(t *testing.T) {
	endpoint := NewNonCooperatingEndpoint()
	endpoint.LoseNextResponse()
	err := endpoint.Apply(context.Background(), "resource-1", []byte(`{"value":1}`))
	if !errors.Is(err, ErrResponseLost) {
		t.Fatalf("lost response error = %v, want %v", err, ErrResponseLost)
	}
	if endpoint.AppliedCount("resource-1") != 1 {
		t.Fatalf("applied count after lost response = %d, want one", endpoint.AppliedCount("resource-1"))
	}
	if err := endpoint.Apply(context.Background(), "resource-1", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	if endpoint.AppliedCount("resource-1") != 2 {
		t.Fatalf("private endpoint oracle count = %d, want two", endpoint.AppliedCount("resource-1"))
	}
}
