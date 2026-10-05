package mcp

import (
	"context"
	"encoding/json"
)

// Tool define o contrato canônico de uma ferramenta MCP executável.
type Tool interface {
	Name() string
	Description() string
	InputSchema() map[string]interface{}
	Execute(ctx context.Context, args json.RawMessage) (interface{}, error)
}

// Resource define um recurso acessível via URI no protocolo MCP.
type Resource interface {
	URI() string
	Name() string
	Description() string
	MimeType() string
	Read(ctx context.Context) ([]byte, error)
}

// Prompt define um template de prompt parametrizável.
type Prompt interface {
	Name() string
	Description() string
	Arguments() []PromptArgument
	Format(ctx context.Context, args map[string]string) (string, error)
}

// PromptArgument descreve os argumentos esperados por um Prompt.
type PromptArgument struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required"`
}

// Registry gerencia o registro e descoberta de Tools, Resources e Prompts.
type Registry interface {
	RegisterTool(tool Tool) error
	GetTool(name string) (Tool, bool)
	ListTools() []ToolDescription

	RegisterResource(res Resource) error
	GetResource(uri string) (Resource, bool)
	ListResources() []ResourceDescription

	RegisterPrompt(prompt Prompt) error
	GetPrompt(name string) (Prompt, bool)
	ListPrompts() []PromptDescription
}

// ToolDescription exporta metadados de uma ferramenta para o cliente MCP.
type ToolDescription struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

// ResourceDescription exporta metadados de um recurso para o cliente MCP.
type ResourceDescription struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

// PromptDescription exporta metadados de um prompt para o cliente MCP.
type PromptDescription struct {
	Name        string           `json:"name"`
	Description string           `json:"description,omitempty"`
	Arguments   []PromptArgument `json:"arguments,omitempty"`
}
