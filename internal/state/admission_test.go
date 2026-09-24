package state

import "testing"

func TestDUR050AdmissionConfigurationIsOptInAndFailClosed(t *testing.T) {
	t.Setenv(dur050AdmissionActiveEnv, "")
	t.Setenv(dur050AdmissionOutboxEnv, "")
	config, err := admissionConfigFromEnvironment()
	if err != nil || config.enabled() {
		t.Fatalf("unset DUR-050 gate config = %+v err=%v, want disabled", config, err)
	}

	t.Setenv(dur050AdmissionActiveEnv, "1000")
	t.Setenv(dur050AdmissionOutboxEnv, "50000")
	config, err = admissionConfigFromEnvironment()
	if err != nil || !config.enabled() || config.MaxActiveWorkflows != 1000 || config.MaxPendingOutbox != 50000 {
		t.Fatalf("configured DUR-050 gate = %+v err=%v", config, err)
	}

	t.Setenv(dur050AdmissionOutboxEnv, "")
	if _, err := admissionConfigFromEnvironment(); err == nil {
		t.Fatal("partial gate configuration was accepted")
	}
	t.Setenv(dur050AdmissionOutboxEnv, "50000")
	t.Setenv(dur050AdmissionActiveEnv, "-1")
	if _, err := admissionConfigFromEnvironment(); err == nil {
		t.Fatal("negative gate limit was accepted")
	}
}

func TestDUR050GateMatchesOnlyCampaignNamespacePrefix(t *testing.T) {
	for _, testCase := range []struct {
		namespace string
		want      bool
	}{
		{namespace: "dur050-run-1", want: true},
		{namespace: "dur050-", want: true},
		{namespace: "dur050", want: false},
		{namespace: "other-dur050-run", want: false},
	} {
		if got := isDur050Namespace(testCase.namespace); got != testCase.want {
			t.Errorf("isDur050Namespace(%q)=%t, want %t", testCase.namespace, got, testCase.want)
		}
	}
}
