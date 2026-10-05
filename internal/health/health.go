// Package health expõe os endpoints de integridade do gateway: /livez diz se o
// processo está vivo; /healthz checa as dependências em paralelo, cada uma com
// prazo, e responde 503 se alguma falhar.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Probe checa uma dependência. Check deve respeitar o cancelamento de ctx;
// uma probe que o ignora é dada como "timeout" no prazo, sem travar a resposta.
type Probe interface {
	Name() string
	Check(ctx context.Context) error
}

type probeFunc struct {
	name  string
	check func(context.Context) error
}

func (p probeFunc) Name() string                    { return p.name }
func (p probeFunc) Check(ctx context.Context) error { return p.check(ctx) }

// NewProbe adapta uma função a Probe.
func NewProbe(name string, check func(context.Context) error) Probe {
	return probeFunc{name: name, check: check}
}

// Status de uma probe e do relatório.
const (
	StatusOK      = "ok"
	StatusFail    = "fail"
	StatusTimeout = "timeout"
)

// Result é o resultado de uma probe. O erro vai só para o log: a resposta é
// pública e o texto de erro de driver costuma carregar host e credencial.
type Result struct {
	Status     string `json:"status"`
	DurationMS int64  `json:"duration_ms"`
}

// Report é o corpo JSON de /livez e /healthz.
type Report struct {
	Status        string            `json:"status"`
	UptimeSeconds int64             `json:"uptime_seconds"`
	Checks        map[string]Result `json:"checks,omitempty"`
}

var errNoAnswer = errors.New("probe não respondeu no prazo")

// Checker atende /livez e /healthz.
type Checker struct {
	log     *slog.Logger
	timeout time.Duration
	probes  []Probe
	started time.Time
}

// New cria o Checker; o uptime conta a partir daqui. Nomes de probe repetidos
// são erro de programação e causam panic.
func New(log *slog.Logger, timeout time.Duration, probes ...Probe) *Checker {
	seen := map[string]bool{}
	for _, p := range probes {
		if seen[p.Name()] {
			panic("health: probe duplicada: " + p.Name())
		}
		seen[p.Name()] = true
	}
	return &Checker{log: log, timeout: timeout, probes: probes, started: time.Now()}
}

// Live responde 200 enquanto o processo atende requisições; não toca em
// dependência, para o orquestrador não reiniciar o gateway por culpa de outro.
func (c *Checker) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, Report{Status: StatusOK, UptimeSeconds: c.uptime()})
}

// Ready roda todas as probes em paralelo, cada uma limitada a timeout, e
// responde 200 se todas passarem ou 503 caso contrário.
func (c *Checker) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), c.timeout)
	defer cancel()

	type outcome struct {
		i   int
		err error
		dur time.Duration
	}
	done := make(chan outcome, len(c.probes)) // buffer: probe atrasada não vaza goroutine bloqueada
	for i, p := range c.probes {
		go func() {
			start := time.Now()
			err := p.Check(ctx)
			done <- outcome{i, err, time.Since(start)}
		}()
	}

	results := make([]Result, len(c.probes))
	errs := make([]error, len(c.probes))
	for i := range results {
		results[i] = Result{Status: StatusTimeout, DurationMS: c.timeout.Milliseconds()}
		errs[i] = errNoAnswer
	}
collect:
	for range c.probes {
		select {
		case o := <-done:
			res := Result{Status: StatusOK, DurationMS: o.dur.Milliseconds()}
			switch {
			case errors.Is(o.err, context.DeadlineExceeded):
				res.Status = StatusTimeout
			case o.err != nil:
				res.Status = StatusFail
			}
			results[o.i], errs[o.i] = res, o.err
		case <-ctx.Done():
			break collect
		}
	}

	rep := Report{Status: StatusOK, UptimeSeconds: c.uptime(), Checks: map[string]Result{}}
	code := http.StatusOK
	for i, res := range results {
		rep.Checks[c.probes[i].Name()] = res
		if res.Status != StatusOK {
			rep.Status, code = StatusFail, http.StatusServiceUnavailable
			c.log.WarnContext(r.Context(), "probe de saúde falhou", "probe", c.probes[i].Name(), "status", res.Status, "error", errs[i])
		}
	}
	writeJSON(w, code, rep)
}

func (c *Checker) uptime() int64 { return int64(time.Since(c.started).Seconds()) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
