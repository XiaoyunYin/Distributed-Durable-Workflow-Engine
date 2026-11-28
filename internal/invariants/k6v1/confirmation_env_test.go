package k6v1

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

var controlActuatorEnv = map[string]bool{
	"DUR048_ACTIVITY_DELAY_MS":                true,
	"DUR049_OWNER_LOCK_ISOLATION_HOLD_MS":     true,
	"DUR049_OWNER_LOCK_ISOLATION_WORKFLOW_ID": true,
	"DUR050_ADMISSION_MAX_ACTIVE":             true,
	"DUR050_ADMISSION_MAX_PENDING_OUTBOX":     true,
	"DUR050_ENABLE_SHA256_ACTIVITY":           true,
	"DUR050_SHA256_WORK_UNITS":                true,
	"DUR053_CPU_WORK_UNITS":                   true,
	"DUR053_F1A_RELEASE_MARKER":               true,
	"DUR053_K4_HOLD_FILE":                     true,
	"DUR053_K6_CHAOS_SMOKE":                   true,
	"DUR053_K6_CLOCK":                         true,
	"DUR053_K6_CLOCK_CONTROL":                 true,
	"DUR053_K6_CLOCK_WORKFLOW":                true,
	"DUR053_K6_DEPLOYED_HOOKS":                true,
	"DUR053_K6_G_OBSERVATION_GATE":            true,
	"DUR053_K6_HOOK_CA_FILE":                  true,
	"DUR053_K6_HOOK_TOKEN":                    true,
	"DUR053_K6_HOOK_URL":                      true,
	"DUR053_K6_HOOK_WORKLOAD":                 true,
	"DUR053_K6_LEASE_TAKEOVER_ROLE":           true,
	"DUR053_K6_LEASE_TAKEOVER_WORKFLOW":       true,
	"DUR053_K6_LEASE_TAKEOVER_CELL":           true,
	"DUR053_K6_PARTITION_CELL":                true,
	"DUR053_K6_PARTITION_WORKFLOW":            true,
	"DUR053_K6_RELAY_ARM":                     true,
	"DUR053_K6_RELAY_STALL":                   true,
	"DURABLE_FAULT_ENDPOINT":                  true,
	"DURABLE_FAULT_SEED":                      true,
	"DURABLE_FAULT_TOKEN":                     true,
	"RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS": true,
	"RUNTIME_SHUTDOWN_MODE":                   true,
	"WORKER_DRAIN_MODE":                       true,
}

// These settings are outside the deployed K6 PodSpec environment. The
// reason records why their readers exist even though controls deny the names.
var envOutsideK6Deployment = map[string]string{
	"CONTAINER_RESTART_COUNT":           "passed to the worker by the per-session G bootstrap",
	"CONTAINER_STARTED_NS":              "passed to the worker by the per-session G bootstrap",
	"DUR029_BUDGET_CENTS":               "incident-agent command, outside the K6 entry points",
	"DUR029_MODEL":                      "incident-agent command, outside the K6 entry points",
	"DUR029_PROVIDER":                   "incident-agent command, outside the K6 entry points",
	"DUR048_RUNTIME_URL":                "DUR-048 load generator",
	"DUR050_OBSERVER_DATABASE_URL":      "DUR-050 observer command",
	"DUR050_RECORD_TRANSACTION_TIMINGS": "DUR-050 transaction timing diagnostics",
	"DUR053_CLIENT_TOKEN_FILE":          "load generator credential, outside the control pods",
	"DUR053_K4_COMMIT_TIMING":           "K4 commit timing diagnostics",
	"DUR053_K6_G_CLAIMANT":              "passed to the worker by the per-session G bootstrap",
	"DUR053_WITNESS_DIR":                "K3/K4 worker witness volume, replaced by the G ledger",
	"DUR055_DATABASE_URL":               "incident-agent retrieval entry point",
	"DUR055_POSTGRES_PORT":              "incident-agent retrieval entry point",
	"DUR056_DATABASE_URL":               "incident-agent retrieval entry point",
	"DUR056_POSTGRES_PORT":              "incident-agent retrieval entry point",
	"DURABLE_DATABASE_URL":              "standalone database fixture and benchmark commands",
	"DURABLE_KAFKA_BROKERS":             "standalone dispatch benchmark",
	"DURABLE_SOURCE_COMMIT":             "DUR-048 load generator metadata",
	"HOSTNAME":                          "process-environment fallback, absent from the PodSpec inventory",
	"INCIDENT_LIVE_APPROVED":            "incident-agent execution entry point",
	"J2_CHECKER_PHASES":                 "J2 checker phase diagnostics outside the control topology",
	"J4_RECORDED_LLM_ACTIVITY":          "J4 fixture opt-in, forbidden in the registered K6 controls",
	"J4_PROVIDER_RESILIENCE":            "J4 provider retry opt-in, forbidden in the registered K6 controls",
	"KAFKA_BROKERS":                     "local demo command",
	"OPENAI_API_KEY":                    "incident-agent provider entry point",
	"OTEL_EXPORTER_OTLP_ENDPOINT":       "optional external exporter, absent in hosted G controls",
	"POSTGRES_DB":                       "incident-agent retrieval fallback",
	"POSTGRES_PASSWORD":                 "incident-agent retrieval fallback",
	"POSTGRES_USER":                     "incident-agent retrieval fallback",
	"RUNTIME_DEMAND_BEARER_TOKEN_FILE":  "demand API role, outside the control topology",
	"RUNTIME_ENGINE_MODE":               "combined-runtime readiness option, unused by the dedicated G roles",
	"RUNTIME_HEALTH_URL":                "runtime health command uses its default in G",
	"RUNTIME_PPROF_ADDR":                "optional profiler listener, disabled in G",
	"RUNTIME_PPROF_ENABLED":             "optional profiler listener, disabled in G",
	"RUNTIME_SCHEDULER":                 "combined-runtime option, G selects its scheduler through RUNTIME_ROLES",
	"WORKER_ADDR":                       "worker health listener uses its default in G",
	"WORKER_HEALTH_URL":                 "worker health command uses its default in G",
	"WORKER_ID":                         "fallback worker identity, G binds the actual pod",
}

func unclassifiedControlEnv(names []string) []string {
	var unknown []string
	for _, name := range names {
		_, legacy := controlLegacyEnv[name]
		classes := []bool{controlConfigEnv[name], controlActuatorEnv[name], envOutsideK6Deployment[name] != "", legacy}
		count := 0
		for _, classified := range classes {
			if classified {
				count++
			}
		}
		if count != 1 {
			unknown = append(unknown, name)
		}
	}
	return unknown
}

func goEnvReadArgument(call *ast.CallExpr) ast.Expr {
	if len(call.Args) == 0 {
		return nil
	}
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		// The runtime and load generator each wrap os.Getenv with a fallback.
		if fn.Name == "envOrDefault" || fn.Name == "envOr" {
			return call.Args[0]
		}
	case *ast.SelectorExpr:
		if pkg, ok := fn.X.(*ast.Ident); ok && (pkg.Name == "os" || pkg.Name == "syscall") &&
			(fn.Sel.Name == "Getenv" || fn.Sel.Name == "LookupEnv") {
			return call.Args[0]
		}
	}
	return nil
}

func literalEnvName(expr ast.Expr) string {
	if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
		name, _ := strconv.Unquote(lit.Value)
		return name
	}
	return ""
}

func goEnvReads(path string, raw []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	constants := map[string]string{}
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.CONST {
			continue
		}
		for _, spec := range group.Specs {
			values := spec.(*ast.ValueSpec)
			for i, value := range values.Values {
				constants[values.Names[i].Name] = literalEnvName(value)
			}
		}
	}
	var names []string
	add := func(name string) {
		if name != "" {
			names = append(names, name)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CallExpr:
			arg := goEnvReadArgument(n)
			if ref, ok := arg.(*ast.Ident); ok {
				add(constants[ref.Name])
			} else {
				add(literalEnvName(arg))
			}
		case *ast.RangeStmt:
			// Several standalone commands try a fixed list of database names.
			list, literal := n.X.(*ast.CompositeLit)
			value, named := n.Value.(*ast.Ident)
			if !literal || !named {
				break
			}
			read := false
			ast.Inspect(n.Body, func(child ast.Node) bool {
				if call, ok := child.(*ast.CallExpr); ok {
					arg, ok := goEnvReadArgument(call).(*ast.Ident)
					read = read || (ok && arg.Name == value.Name)
				}
				return !read
			})
			if read {
				for _, item := range list.Elts {
					add(literalEnvName(item))
				}
			}
		}
		return true
	})
	return names, nil
}

var pythonEnvReader = regexp.MustCompile(`\b(?:os\.)?(?:(?:getenv|environ\.get)\s*\(|environ\s*\[)\s*["']([^"'\r\n]+)["']`)

func TestK6ConfirmationEnvReadersClassified(t *testing.T) {
	t.Run("RecordedProviderForbidden", func(t *testing.T) {
		for _, name := range []string{"J4_RECORDED_LLM_ACTIVITY", "J4_PROVIDER_RESILIENCE"} {
			snapshot := ControlSnapshot{Pods: []ControlPod{{EnvNames: []string{name}}}}
			if !snapshot.faultConfigured() {
				t.Fatalf("%s was admitted into a K6 control", name)
			}
		}
	})
	t.Run("WrapperFixture", func(t *testing.T) {
		raw := []byte(`package runtime
var value = envOrDefault("X", "default")`)
		names, err := goEnvReads("fixture.go", raw)
		if err != nil || !slices.Equal(unclassifiedControlEnv(names), []string{"X"}) {
			t.Fatalf("unclassified wrapper read escaped: %v (%v)", names, err)
		}
	})
	for _, directory := range []string{"cmd", "internal", "python", "campaign", "scripts/dur053_k6_g_worker.py"} {
		err := filepath.WalkDir(filepath.Join("../../..", directory), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			if entry.IsDir() || strings.HasSuffix(path, "_test.go") || (ext != ".go" && ext != ".py") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var names []string
			if ext == ".go" {
				names, err = goEnvReads(path, raw)
				if err != nil {
					return err
				}
			} else {
				for _, match := range pythonEnvReader.FindAllStringSubmatch(string(raw), -1) {
					names = append(names, match[1])
				}
			}
			for _, name := range unclassifiedControlEnv(names) {
				t.Errorf("%s reads %s, which needs exactly one control environment classification", path, name)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
