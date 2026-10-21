// Command load é o alvo da carga do k6 (chat.js, nesta pasta): o gateway sobre
// os fakes do internal/testutil/gatewaytest, de pé até SIGINT/SIGTERM. A
// configuração é a do gateway (flags, FLOW_*, .env); a OpenRouter de mentira se
// regula por --ttft, --gap e --chunks (o padrão: um stream de ~4 s).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/testutil/gatewaytest"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := pflag.NewFlagSet("load", pflag.ContinueOnError)
	config.RegisterFlags(fs)
	var f gatewaytest.Fakes
	fs.DurationVar(&f.TTFT, "ttft", 250*time.Millisecond, "espera da OpenRouter de mentira até o 1º pedaço")
	fs.DurationVar(&f.Gap, "gap", 200*time.Millisecond, "intervalo entre os pedaços seguintes")
	fs.IntVar(&f.Chunks, "chunks", 20, "pedaços por resposta")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(fs)
	if err != nil {
		return err
	}
	log, err := logger.New(os.Stdout, cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := gatewaytest.Start(cfg, f, log)
	if err != nil {
		return err
	}
	log.Info("alvo de carga no ar", "url", st.URL)
	<-ctx.Done()
	return st.Close()
}
