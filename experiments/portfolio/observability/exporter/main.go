package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const interval = time.Second
const stale = 5 * interval

var windows = [...]string{"2m", "10m", "all"}

type counts struct {
	Eligible int64 `json:"eligible"`
	Bad      int64 `json:"bad"`
}

type projection struct {
	At          int64             `json:"sample_ns"`
	Uncertainty int64             `json:"u_ns"`
	Windows     map[string]counts `json:"windows"`
	Admissions  map[string]string `json:"admissions"`
	Successes   map[string]int64  `json:"successes"`
	Outstanding int64             `json:"outstanding"`
	Unpublished int64             `json:"unpublished"`
	SQLAge      float64           `json:"sql_oldest_claimable_seconds"`
}

type demand struct {
	Namespace string    `json:"namespace"`
	At        time.Time `json:"sampled_at"`
	Age       *float64  `json:"oldest_claimable_age_seconds"`
	Claimable *int64    `json:"claimable_attempts"`
	Active    *int64    `json:"active_claims"`
}

func fresh(at time.Time, now time.Time) bool {
	return !at.IsZero() && !at.After(now) && now.Sub(at) <= stale
}

func readDemand(ctx context.Context, client *http.Client, url, token, namespace string) (*demand, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("demand HTTP %d", response.StatusCode)
	}
	var d demand
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4096))
	if err = decoder.Decode(&d); err != nil {
		return nil, err
	}
	if decoder.Decode(new(any)) != io.EOF || d.Namespace != namespace || !fresh(d.At, time.Now()) ||
		d.Age == nil || d.Claimable == nil || d.Active == nil ||
		*d.Age < 0 || math.IsInf(*d.Age, 0) || *d.Claimable < 0 || *d.Active < 0 ||
		(*d.Claimable == 0 && *d.Age != 0) {
		return nil, errors.New("invalid demand sample")
	}
	return &d, nil
}

func readProjection(ctx context.Context, db *sql.DB) (*projection, error) {
	var raw []byte
	if err := db.QueryRowContext(ctx, "SELECT j2_monitor.j3_sample()").Scan(&raw); err != nil {
		return nil, err
	}
	var p projection
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	if !fresh(time.Unix(0, p.At), time.Now()) || p.Admissions == nil || p.Successes == nil {
		return nil, errors.New("missing or stale projection")
	}
	for _, name := range windows {
		c, found := p.Windows[name]
		if !found || c.Bad < 0 || c.Eligible < c.Bad {
			return nil, errors.New("invalid deadline cohort")
		}
	}
	return &p, nil
}

type changes[V comparable] struct {
	Set    map[string]V `json:"set"`
	Delete []string     `json:"delete"`
}

func delta[V comparable](before, after map[string]V) changes[V] {
	d := changes[V]{Set: map[string]V{}, Delete: []string{}}
	for id, value := range after {
		if old, found := before[id]; !found || old != value {
			d.Set[id] = value
		}
	}
	for id := range before {
		if _, found := after[id]; !found {
			d.Delete = append(d.Delete, id)
		}
	}
	return d
}

type sample struct {
	sql    *projection
	demand *demand
}

type sampler struct {
	latest   atomic.Pointer[sample]
	evidence *os.File
	previous *projection
}

func (s *sampler) collect(ctx context.Context, sqlRead func(context.Context) (*projection, error), demandRead func(context.Context) (*demand, error)) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	p, sqlErr := sqlRead(ctx)
	d, demandErr := demandRead(ctx)
	if sqlErr != nil || demandErr != nil {
		log.Printf("monitor cycle failed: sql_ok=%t demand_ok=%t", sqlErr == nil, demandErr == nil)
	}
	if p != nil {
		evidence := struct {
			At          int64             `json:"sample_ns"`
			Uncertainty int64             `json:"u_ns"`
			Windows     map[string]counts `json:"windows"`
			Admissions  changes[string]   `json:"admissions"`
			Successes   changes[int64]    `json:"successes"`
			Demand      *demand           `json:"demand"`
		}{p.At, p.Uncertainty, p.Windows, delta(s.previous.Admissions, p.Admissions),
			delta(s.previous.Successes, p.Successes), d}
		if err := json.NewEncoder(s.evidence).Encode(evidence); err != nil {
			return err
		}
		if err := s.evidence.Sync(); err != nil {
			return err
		}
		s.previous = p
	}
	s.latest.Store(&sample{sql: p, demand: d})
	return nil
}

func (s *sampler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	value := s.latest.Load()
	now := time.Now()
	if value == nil {
		value = &sample{}
	}
	p, d := value.sql, value.demand
	sqlOK := p != nil && fresh(time.Unix(0, p.At), now)
	demandOK := d != nil && fresh(d.At, now)
	fmt.Fprintf(w, "j3_sql_ok %d\nj3_demand_ok %d\n", flag(sqlOK), flag(demandOK))
	if sqlOK {
		fmt.Fprintf(w, "j3_sample_timestamp_seconds %d.%09d\n", p.At/1e9, p.At%1e9)
		fmt.Fprintf(w, "j3_outstanding %d\nj3_unpublished %d\nj3_sql_oldest_claimable_seconds %g\n",
			p.Outstanding, p.Unpublished, p.SQLAge)
		for _, name := range windows {
			c := p.Windows[name]
			fmt.Fprintf(w, "j3_eligible{window=%q} %d\nj3_bad{window=%q} %d\n", name, c.Eligible, name, c.Bad)
		}
	}
	if demandOK {
		fmt.Fprintf(w, "j3_demand_timestamp_seconds %d.%09d\n", d.At.Unix(), d.At.Nanosecond())
		fmt.Fprintf(w, "j3_oldest_claimable_age_seconds %g\nj3_claimable_attempts %d\nj3_active_claims %d\n",
			*d.Age, *d.Claimable, *d.Active)
	}
}

func flag(ok bool) int {
	if ok {
		return 1
	}
	return 0
}

func database(dsn string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["application_name"] = "j3-db-exporter"
	config.RuntimeParams["statement_timeout"] = "2000"
	config.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func main() {
	db, err := database(os.Getenv("J3_MONITOR_DSN"))
	if err != nil {
		log.Fatal("invalid monitor DSN")
	}
	defer db.Close()
	url, token, namespace := os.Getenv("J3_DEMAND_URL"), os.Getenv("J3_DEMAND_TOKEN"), os.Getenv("J3_NAMESPACE")
	if url == "" || token == "" || namespace == "" {
		log.Fatal("missing demand configuration")
	}
	file, err := os.OpenFile("/evidence/visibility.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal("cannot create new visibility evidence")
	}
	defer file.Close()
	s := &sampler{evidence: file, previous: new(projection)}
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	go func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for range tick.C {
			if err := s.collect(context.Background(),
				func(ctx context.Context) (*projection, error) { return readProjection(ctx, db) },
				func(ctx context.Context) (*demand, error) { return readDemand(ctx, client, url, token, namespace) }); err != nil {
				log.Fatal("cannot retain snapshot visibility evidence")
			}
		}
	}()
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /visibility.jsonl", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, file.Name())
		})
		server := http.Server{Addr: "127.0.0.1:9188", Handler: mux, ReadHeaderTimeout: 2 * time.Second}
		log.Fatal(server.ListenAndServe())
	}()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", s)
	server := http.Server{Addr: ":9187", Handler: mux, ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout: 2 * time.Second, IdleTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
