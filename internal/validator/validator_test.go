package validator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
)

const tree = `{"$defs":{"node":{"type":"object","properties":{"kids":{"type":"array","items":{"$ref":"#/$defs/node"}}},"additionalProperties":false}},"$ref":"#/$defs/node"}`

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		schema, args string
		ok           bool
	}{
		{`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`, `{"q":"oi"}`, true},
		{`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`, `{}`, false},
		{`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`, `{"q":1}`, false},
		{`{"type":"object","required":["q"]}`, ``, false}, // vazio vale {}
		{`{"type":"object"}`, `  `, true},
		{`{"properties":{"q":{"type":"string"}},"additionalProperties":false}`, `{"q":"a","x":1}`, false},
		{`{"additionalProperties":{"type":"integer"}}`, `{"a":1,"b":2.0}`, true},
		{`{"additionalProperties":{"type":"integer"}}`, `{"a":1.5}`, false},
		{`{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":2}`, `["a","b"]`, true},
		{`{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":2}`, `[]`, false},
		{`{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":2}`, `["a",2]`, false},
		{`{"type":"string","minLength":3,"maxLength":3}`, `"ção"`, true}, // conta caracteres, não bytes
		{`{"type":"string","minLength":3,"maxLength":3}`, `"çã"`, false},
		{`{"type":"string","pattern":"^[a-z]+$"}`, `"abc"`, true},
		{`{"type":"string","pattern":"^[a-z]+$"}`, `"ab1"`, false},
		{`{"enum":["a",1,true,null]}`, `1.0`, true}, // número pelo valor
		{`{"enum":["a",1,true,null]}`, `"b"`, false},
		{`{"enum":["a",1,true,null]}`, `{"a":1}`, false},
		{`{"const":"fixo"}`, `"outro"`, false},
		{`{"type":"integer","minimum":1,"maximum":10}`, `10`, true},
		{`{"type":"integer","minimum":1,"maximum":10}`, `11`, false},
		{`{"type":"number","exclusiveMinimum":0}`, `0`, false},
		{`{"type":"number","maximum":10}`, `1e400`, false}, // fora do float64: +Inf
		{`{"type":["string","null"]}`, `null`, true},
		{`{"anyOf":[{"type":"string"},{"type":"null"}]}`, `"a"`, true},
		{`{"anyOf":[{"type":"string"},{"type":"null"}]}`, `1`, false},
		{`{"properties":{"x":false}}`, `{"x":1}`, false},
		{tree, `{"kids":[{"kids":[{}]},{}]}`, true}, // $ref recursivo desce com os dados
		{tree, `{"kids":[{"kids":[{"nome":1}]}]}`, false},
		{`{"type":"object"}`, `{"a":1} {}`, false}, // dados depois do valor
		{`{"type":"object"}`, `{"a":`, false},
	} {
		s, err := compile([]byte(tc.schema))
		require.NoError(t, err, tc.schema)
		err = s.Validate([]byte(tc.args))
		if tc.ok {
			assert.NoError(t, err, "%s com %s", tc.schema, tc.args)
		} else {
			assert.ErrorIs(t, err, gwerrors.ErrInvalidPayload, "%s com %s", tc.schema, tc.args)
		}
	}
}

func TestCompileFailsClosed(t *testing.T) {
	for schema, want := range map[string]string{
		`{"oneOf":[{"type":"string"}]}`:                "#/oneOf: palavra-chave não suportada",
		`{"properties":{"a":{"uniqueItems":true}}}`:    "#/properties/a/uniqueItems: palavra-chave não suportada",
		`{"type":"string","pattern":"(?=x)"}`:          "#/pattern",
		`{"$ref":"#/$defs/falta"}`:                     `$ref "#/$defs/falta" não encontrado`,
		`{"$ref":"https://exemplo.com/s.json"}`:        "não encontrado",
		`{"properties":{"a":{"$defs":{}}}}`:            "$defs só na raiz",
		`{"type":"array","items":[{"type":"string"}]}`: "#/items: schema precisa ser objeto ou booleano",
		`{"enum":[{"a":1}]}`:                           "#/enum",
		`{"type":"texto"}`:                             "tipo texto desconhecido",
		`{"exclusiveMinimum":true}`:                    "#/exclusiveMinimum: precisa ser número",
	} {
		_, err := compile([]byte(schema))
		require.Error(t, err, schema)
		assert.Contains(t, err.Error(), want)
		assert.NotErrorIs(t, err, gwerrors.ErrInvalidPayload, "schema ruim é erro do servidor, não do cliente")
	}
}

func TestLimitsAndCycles(t *testing.T) {
	s, err := compile([]byte(`{}`))
	require.NoError(t, err)
	assert.NoError(t, s.Validate([]byte(strings.Repeat("[", maxDepth)+strings.Repeat("]", maxDepth))))
	assert.ErrorIs(t, s.Validate([]byte(strings.Repeat("[", maxDepth+1)+strings.Repeat("]", maxDepth+1))), gwerrors.ErrInvalidPayload)
	assert.ErrorIs(t, s.Validate([]byte(`"`+strings.Repeat("a", maxBytes)+`"`)), gwerrors.ErrInvalidPayload)

	loop, err := compile([]byte(`{"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"}},"$ref":"#/$defs/a"}`))
	require.NoError(t, err)
	assert.ErrorIs(t, loop.Validate([]byte(`{}`)), gwerrors.ErrInvalidPayload, "ciclo de $ref vira erro, não estouro de pilha")
}

// tool é uma mcp.Tool com o schema trocável.
type tool struct{ schema map[string]any }

func (t *tool) Name() string                                          { return "busca" }
func (t *tool) Description() string                                   { return "" }
func (t *tool) InputSchema() map[string]any                           { return t.schema }
func (t *tool) Execute(context.Context, json.RawMessage) (any, error) { return nil, nil }

func TestCacheCompilesOncePerSchemaVersion(t *testing.T) {
	var c Cache
	tl := &tool{schema: map[string]any{"type": "object", "required": []string{"q"}}} // tipos Go, como as tools do pkg/mcp
	require.NoError(t, c.Validate(tl, json.RawMessage(`{"q":"oi"}`)))
	assert.ErrorIs(t, c.Validate(tl, json.RawMessage(`{}`)), gwerrors.ErrInvalidPayload)
	first, _ := c.m.Load("busca")

	require.NoError(t, c.Validate(tl, json.RawMessage(`{"q":"de novo"}`)))
	again, _ := c.m.Load("busca")
	assert.Same(t, first.(cached).schema, again.(cached).schema, "o mesmo schema não recompila")

	tl.schema = map[string]any{"type": "object", "required": []string{"id"}}
	assert.ErrorIs(t, c.Validate(tl, json.RawMessage(`{"q":"oi"}`)), gwerrors.ErrInvalidPayload, "schema novo recompila")
	tl.schema = map[string]any{"type": "object", "oneOf": []any{}}
	assert.NotErrorIs(t, c.Validate(tl, json.RawMessage(`{}`)), gwerrors.ErrInvalidPayload, "schema que não compila falha fechado")
	tl.schema = map[string]any{"x": func() {}}
	assert.Error(t, c.Validate(tl, nil))
}
