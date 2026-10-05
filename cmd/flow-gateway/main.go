// Command flow-gateway é o ponto de entrada do gateway.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/pflag"

	"github.com/Educacao-Solidaria/flow-gateway-go/internal/config"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/logger"
	"github.com/Educacao-Solidaria/flow-gateway-go/internal/version"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
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
	return nil
}
