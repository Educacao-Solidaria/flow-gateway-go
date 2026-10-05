package domain

// Modelos conhecidos e canônicos suportados no ecossistema via OpenRouter.
const (
	ModelDeepSeekV3 = "deepseek/deepseek-chat"
	ModelDeepSeekR1 = "deepseek/deepseek-r1"
	ModelQwen25     = "qwen/qwen-2.5-72b-instruct"
	ModelLlama33    = "meta-llama/llama-3.3-70b-instruct"
)

// Role define o papel do autor de uma mensagem no chat.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ChatMessage representa uma mensagem estruturada no formato compatível com OpenAI/OpenRouter.
type ChatMessage struct {
	Role       Role       `json:"role"`
	Content    string     `json:"content"`
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall representa uma invocação de ferramenta solicitada pelo modelo.
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

// FunctionCall detalha a função e os argumentos serializados em JSON.
type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ChatCompletionRequest representa a carga enviada para a API da OpenRouter.
type ChatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Stream      bool          `json:"stream,omitempty"`
	Temperature float32       `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Tools       []Tool        `json:"tools,omitempty"`
	TenantID    string        `json:"tenant_id,omitempty"`
}

// Tool descreve uma ferramenta disponível para o modelo invocar.
type Tool struct {
	Type     string             `json:"type"`
	Function FunctionDefinition `json:"function"`
}

// FunctionDefinition especifica os metadados e schema de uma função.
type FunctionDefinition struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Parameters  map[string]interface{} `json:"parameters"`
}

// ChatCompletionResponse representa a resposta completa retornada pelo upstream.
type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// Choice representa uma das respostas geradas pelo modelo.
type Choice struct {
	Index        int          `json:"index"`
	Message      *ChatMessage `json:"message,omitempty"`
	Delta        *ChatMessage `json:"delta,omitempty"`
	FinishReason string       `json:"finish_reason,omitempty"`
}

// Usage contabiliza os tokens consumidos na requisição.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}
