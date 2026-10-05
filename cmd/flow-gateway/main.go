// Command flow-gateway é o ponto de entrada do gateway.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/server"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/version"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run sobe o gateway e bloqueia até ctx ser cancelado (SIGINT/SIGTERM).
func run(ctx context.Context, args []string, stdout io.Writer) error {
	fs := pflag.NewFlagSet("flow-gateway", pflag.ContinueOnError)
	fs.SetOutput(io.Discard)
	config.RegisterFlags(fs)
	showVersion := fs.Bool("version", false, "imprime a versão e sai")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		_, err := fmt.Fprintln(stdout, version.String())
		return err
	}
	cfg, err := config.Load(fs)
	if err != nil {
		return err
	}
	log, err := logger.New(stdout, cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return err
	}
	log.Info("configuração carregada", "version", version.Version, "env", cfg.Env, "addr", cfg.Server.Addr)
	srv, err := server.New(cfg.Server, log)
	if err != nil {
		return err
	}
	return srv.Run(ctx)
}
