package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// SSEEvent representa um evento individual no protocolo Server-Sent Events (SSE).
type SSEEvent struct {
	ID    string      `json:"id,omitempty"`
	Event string      `json:"event,omitempty"`
	Data  interface{} `json:"data"`
	Retry int         `json:"retry,omitempty"`
}

// Format serializa o evento no padrão canônico SSE ("data: ...\n\n").
func (e *SSEEvent) Format() ([]byte, error) {
	var buf bytes.Buffer

	if e.ID != "" {
		fmt.Fprintf(&buf, "id: %s\n", e.ID)
	}
	if e.Event != "" {
		fmt.Fprintf(&buf, "event: %s\n", e.Event)
	}
	if e.Retry > 0 {
		fmt.Fprintf(&buf, "retry: %d\n", e.Retry)
	}

	dataBytes, err := json.Marshal(e.Data)
	if err != nil {
		return nil, fmt.Errorf("falha ao serializar SSE data: %w", err)
	}

	fmt.Fprintf(&buf, "data: %s\n\n", dataBytes)
	return buf.Bytes(), nil
}

// StreamChunk encapsula uma fração de texto recebida durante o streaming do LLM.
type StreamChunk struct {
	ID           string `json:"id"`
	Model        string `json:"model"`
	DeltaContent string `json:"delta_content"`
	FinishReason string `json:"finish_reason,omitempty"`
	IsLast       bool   `json:"is_last"`
}
