# Flow Gateway Go

> Gateway de mensageria em tempo real e proxy de alta concorrencia para OpenRouter com servidor MCP (Model Context Protocol) nativo em Go.

## Visao Geral

O **Flow Gateway Go** e o componente de comunicacao em tempo real do ecossistema. Ele atua como um multiplexador de streaming SSE e barramento de eventos de alta vazao, desacoplando clientes de mensageria e servindo como um Servidor MCP nativo de conexao com modelos de IA.

## Como Funciona



1. **Proxy OpenRouter:** Gerencia conexoes de streaming (Server-Sent Events) com backpressure, rate-limiting por tenant e failover interno de provedores.
2. **Servidor MCP:** Expõe ferramentas (tools) para despacho de mensagens, inspecao de streams e controle de sessoes conversacionais.
3. **Alta Concorrencia:** Construido em Go para suportar mais de 50.000 conexoes simultaneas com uso minimo de CPU e memoria.

## Arquitetura & Modulos

O desenvolvimento e guiado pelo roadmap de **100 PRs** no [Plane da in100tiva (GATEGO)](https://plane.in100tiva.com/in100tiva/):

- **Fase 1:** Fundacao, CI/CD e Contratos (PRs 01-20)
- **Fase 2:** Core Engine e Protocolo MCP (PRs 21-50)
- **Fase 3:** Adaptadores, Conectores e Streaming (PRs 51-75)
- **Fase 4:** Observabilidade OTel e Benchmarks de Latencia (PRs 76-90)
- **Fase 5:** Release v1.0, Docker e Documentacao (PRs 91-100+)

## Regras de Engenharia

- **Tamanho dos PRs:** Minimo 100 linhas, maximo 500 linhas de codigo.
- **Commits:** Atomicos seguindo padrao Conventional Commits.
- **Contract-First:** Schemas e interfaces definidos primeiro para trabalho paralelo sem bloqueios.

## Mantenedores

- **Luan Oliveira** ([@in100tiva](https://github.com/in100tiva)) — Arquiteto de Software & Tech Lead
- **Victor Nascimento** ([@VictorNascimento14](https://github.com/VictorNascimento14)) — Tech Lead & Engenheiro de Software
