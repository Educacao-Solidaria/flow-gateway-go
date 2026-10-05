package mcp

import (
	"fmt"
	"sync"
)

// InMemoryRegistry é uma implementação thread-safe de Registry.
type InMemoryRegistry struct {
	mu        sync.RWMutex
	tools     map[string]Tool
	resources map[string]Resource
	prompts   map[string]Prompt
}

// NewInMemoryRegistry cria uma nova instância de InMemoryRegistry.
func NewInMemoryRegistry() *InMemoryRegistry {
	return &InMemoryRegistry{
		tools:     make(map[string]Tool),
		resources: make(map[string]Resource),
		prompts:   make(map[string]Prompt),
	}
}

// RegisterTool registra uma nova ferramenta no catálogo.
func (r *InMemoryRegistry) RegisterTool(tool Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := tool.Name()
	if name == "" {
		return fmt.Errorf("nome da tool nao pode ser vazio")
	}
	if _, exists := r.tools[name]; exists {
		return fmt.Errorf("tool com nome '%s' ja registrada", name)
	}

	r.tools[name] = tool
	return nil
}

// GetTool recupera uma ferramenta pelo nome.
func (r *InMemoryRegistry) GetTool(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tool, exists := r.tools[name]
	return tool, exists
}

// ListTools retorna todas as ferramentas cadastradas.
func (r *InMemoryRegistry) ListTools() []ToolDescription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]ToolDescription, 0, len(r.tools))
	for _, t := range r.tools {
		list = append(list, ToolDescription{
			Name:        t.Name(),
			Description: t.Description(),
			InputSchema: t.InputSchema(),
		})
	}
	return list
}

// RegisterResource registra um recurso indexado por URI.
func (r *InMemoryRegistry) RegisterResource(res Resource) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	uri := res.URI()
	if uri == "" {
		return fmt.Errorf("uri do recurso nao pode ser vazia")
	}
	if _, exists := r.resources[uri]; exists {
		return fmt.Errorf("recurso com uri '%s' ja registrado", uri)
	}

	r.resources[uri] = res
	return nil
}

// GetResource busca um recurso por URI.
func (r *InMemoryRegistry) GetResource(uri string) (Resource, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res, exists := r.resources[uri]
	return res, exists
}

// ListResources lista metadados dos recursos registrados.
func (r *InMemoryRegistry) ListResources() []ResourceDescription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]ResourceDescription, 0, len(r.resources))
	for _, res := range r.resources {
		list = append(list, ResourceDescription{
			URI:         res.URI(),
			Name:        res.Name(),
			Description: res.Description(),
			MimeType:    res.MimeType(),
		})
	}
	return list
}

// RegisterPrompt registra um prompt catalogado.
func (r *InMemoryRegistry) RegisterPrompt(prompt Prompt) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := prompt.Name()
	if name == "" {
		return fmt.Errorf("nome do prompt nao pode ser vazio")
	}
	if _, exists := r.prompts[name]; exists {
		return fmt.Errorf("prompt com nome '%s' ja registrado", name)
	}

	r.prompts[name] = prompt
	return nil
}

// GetPrompt busca um template de prompt pelo nome.
func (r *InMemoryRegistry) GetPrompt(name string) (Prompt, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	prompt, exists := r.prompts[name]
	return prompt, exists
}

// ListPrompts lista metadados dos prompts registrados.
func (r *InMemoryRegistry) ListPrompts() []PromptDescription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]PromptDescription, 0, len(r.prompts))
	for _, p := range r.prompts {
		list = append(list, PromptDescription{
			Name:        p.Name(),
			Description: p.Description(),
			Arguments:   p.Arguments(),
		})
	}
	return list
}
