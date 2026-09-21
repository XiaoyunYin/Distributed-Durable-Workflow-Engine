package reference

import (
	"errors"
	"testing"
)

func TestReferenceModelSafetyOrderings(t *testing.T) {
	tests := []struct {
		name string
		ops  []Operation
		want WorkflowState
	}{
		{name: "result before timeout wins", ops: []Operation{
			{Kind: Claim, Owner: "owner-a", Epoch: 1},
			{Kind: Complete, Owner: "owner-a", Epoch: 1, Attempt: 1},
			{Kind: Timeout, Owner: "owner-a", Epoch: 1, Attempt: 1, Effect: PureEffect},
		}, want: Succeeded},
		{name: "non cooperating timeout reconciles", ops: []Operation{
			{Kind: Claim, Owner: "owner-a", Epoch: 1},
			{Kind: Timeout, Owner: "owner-a", Epoch: 1, Attempt: 1, Effect: UnknownEffect},
		}, want: ReconciliationRequired},
		{name: "cancellation prevents late result", ops: []Operation{
			{Kind: Claim, Owner: "owner-a", Epoch: 1},
			{Kind: Cancel, Owner: "owner-a", Epoch: 1, Attempt: 1, Effect: CooperatingEffect},
		}, want: Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := New()
			for _, op := range test.ops {
				_ = model.Apply(op)
				if err := model.Validate(); err != nil {
					t.Fatalf("after %#v: %v", op, err)
				}
			}
			if got := model.Snapshot().State; got != test.want {
				t.Fatalf("state = %s, want %s", got, test.want)
			}
		})
	}

	model := New()
	if err := model.Apply(Operation{Kind: Takeover, NewOwner: "owner-b", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	before := model.Snapshot()
	if err := model.Apply(Operation{Kind: Acknowledge, Owner: "owner-a", Epoch: 1}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale acknowledgement = %v, want ErrStaleLease", err)
	}
	if got := model.Snapshot(); got != before {
		t.Fatalf("stale owner mutated model: before=%+v after=%+v", before, got)
	}
}

func FuzzReferenceModelSequences(f *testing.F) {
	f.Add([]byte{0, 0, 1, 1, 2, 3, 4, 5})
	f.Add([]byte{0, 0, 2, 2, 3, 1, 5, 4, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256 {
			data = data[:256]
		}
		model := New()
		for index, value := range data {
			kind := OpKind(value % 6)
			owner, epoch := "owner-a", int64(1)
			if value&0x40 != 0 {
				owner, epoch = "owner-b", 2
			}
			effect := PureEffect
			if value%3 == 1 {
				effect = CooperatingEffect
			} else if value%3 == 2 {
				effect = UnknownEffect
			}
			op := Operation{Kind: kind, Owner: owner, Epoch: epoch,
				Attempt: int(value%3 + 1), Effect: effect}
			if kind == Takeover {
				op.Owner, op.NewOwner, op.Epoch = "", "owner-b", int64(2+index)
			}
			_ = model.Apply(op)
			if err := model.Validate(); err != nil {
				t.Fatalf("sequence index %d op=%+v: %v", index, op, err)
			}
		}
	})
}
