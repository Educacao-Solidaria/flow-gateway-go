package health

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func ready(t *testing.T, c *Checker) (int, Report) {
	t.Helper()
	rec := httptest.NewRecorder()
	c.Ready(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var rep Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
		t.Fatalf("corpo não é JSON: %v: %s", err, rec.Body)
	}
	return rec.Code, rep
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)) }

func ok(context.Context) error { return nil }

func TestLive(t *testing.T) {
	c := New(discard(), time.Second, NewProbe("falha", func(context.Context) error { return errors.New("x") }))
	c.started = time.Now().Add(-90 * time.Second)
	rec := httptest.NewRecorder()
	c.Live(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("liveness não deve depender das probes: %d", rec.Code)
	}
	if want := `{"status":"ok","uptime_seconds":90}`; strings.TrimSpace(rec.Body.String()) != want {
		t.Errorf("corpo = %s, want %s", rec.Body, want)
	}
}

func TestReadyAllOK(t *testing.T) {
	code, rep := ready(t, New(discard(), time.Second, NewProbe("cache", ok), NewProbe("rag", ok)))
	if code != http.StatusOK || rep.Status != StatusOK || len(rep.Checks) != 2 {
		t.Fatalf("esperava 200 com 2 checks ok: %d %+v", code, rep)
	}
}

func TestReadyNoProbes(t *testing.T) {
	code, rep := ready(t, New(discard(), time.Second))
	if code != http.StatusOK || rep.Status != StatusOK || rep.Checks != nil {
		t.Fatalf("sem probes: %d %+v", code, rep)
	}
}

func TestReadyFailureHidesErrorText(t *testing.T) {
	var logs bytes.Buffer
	c := New(slog.New(slog.NewJSONHandler(&logs, nil)), time.Second,
		NewProbe("cache", ok),
		NewProbe("db", func(context.Context) error { return errors.New("dial postgres://user:senha@db:5432") }))

	rec := httptest.NewRecorder()
	c.Ready(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "senha") {
		t.Errorf("erro da probe vazou na resposta pública: %s", rec.Body)
	}
	if !strings.Contains(logs.String(), `"probe":"db"`) {
		t.Errorf("falha da probe não foi logada: %s", logs.String())
	}
	var rep Report
	_ = json.Unmarshal(rec.Body.Bytes(), &rep)
	if rep.Checks["db"].Status != StatusFail || rep.Checks["cache"].Status != StatusOK {
		t.Errorf("checks inesperados: %+v", rep.Checks)
	}
}

func TestReadyRunsProbesConcurrently(t *testing.T) {
	slow := func(ctx context.Context) error {
		select {
		case <-time.After(150 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c := New(discard(), time.Second, NewProbe("a", slow), NewProbe("b", slow), NewProbe("c", slow))
	start := time.Now()
	code, _ := ready(t, c)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Errorf("probes rodaram em série: %v", d)
	}
}

func TestReadyTimeout(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	var logs bytes.Buffer
	c := New(slog.New(slog.NewJSONHandler(&logs, nil)), 50*time.Millisecond,
		NewProbe("ignora-ctx", func(context.Context) error { <-block; return nil }),
		NewProbe("respeita-ctx", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }),
		NewProbe("rapida", ok))

	start := time.Now()
	code, rep := ready(t, c)
	if time.Since(start) > time.Second {
		t.Fatal("probe que ignora o contexto travou a resposta")
	}
	if code != http.StatusServiceUnavailable || rep.Status != StatusFail {
		t.Fatalf("esperava 503/fail: %d %+v", code, rep)
	}
	for name, want := range map[string]string{"ignora-ctx": StatusTimeout, "respeita-ctx": StatusTimeout, "rapida": StatusOK} {
		if got := rep.Checks[name].Status; got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if !strings.Contains(logs.String(), `"probe":"ignora-ctx"`) {
		t.Errorf("probe sem resposta não foi logada: %s", logs.String())
	}
}

func TestNewPanicsOnDuplicateProbe(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("esperava panic com probe duplicada")
		}
	}()
	New(discard(), time.Second, NewProbe("x", ok), NewProbe("x", ok))
}
