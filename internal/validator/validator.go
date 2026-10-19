// Package validator confere os argumentos de uma ferramenta MCP contra o
// InputSchema dela sem reflexão no caminho dos dados: o schema vira uma árvore
// de regras uma vez, e o JSON não confiável, map/slice/float64 conferidos por
// type switch.
//
// Subconjunto (o que zod-to-json-schema e pydantic geram para tools): type (nome
// ou lista), enum e const (escalares), properties, required,
// additionalProperties, items (um schema), minItems, maxItems, minLength,
// maxLength, pattern (RE2), minimum, maximum, exclusiveMinimum,
// exclusiveMaximum, anyOf, schemas booleanos e $ref local (#, #/$defs/x,
// #/definitions/x; $defs só na raiz). Anotações ignoradas: $schema, $comment,
// title, description, default, examples, format, deprecated, readOnly,
// writeOnly. Outra palavra-chave (allOf, oneOf, not, uniqueItems...) é erro de
// compilação: aceitá-la em silêncio deixaria passar o que o schema quis barrar.
package validator

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"unicode/utf8"

	gwerrors "github.com/Educacao-Solidaria/flow-gateway-go/pkg/errors"
	"github.com/Educacao-Solidaria/flow-gateway-go/pkg/mcp"
)

// Limites do JSON não confiável. ponytail: fixos; viram config se alguma
// ferramenta precisar de mais.
const (
	maxBytes = 1 << 20 // tamanho dos argumentos
	maxDepth = 64      // aninhamento dos dados somado aos saltos de $ref e anyOf (um ciclo para aqui)
)

// Bits de tipo; número sem parte fracionária (1.0 inclusive) é integer.
// ponytail: números em float64 (o que o encoding/json dá); acima de 2^53 a
// fração se perde, e um número fora do float64 é JSON inválido.
const (
	tNull uint8 = 1 << iota
	tBool
	tString
	tNumber
	tInteger
	tArray
	tObject
	tNone // schema false: nenhum valor tem esse tipo
)

var (
	typeBits  = map[string]uint8{"null": tNull, "boolean": tBool, "string": tString, "number": tNumber | tInteger, "integer": tInteger, "array": tArray, "object": tObject}
	typeNames = [...]string{"null", "boolean", "string", "number", "integer", "array", "object"}
)

// node é um schema compilado; limite ausente é nil.
type node struct {
	types                                uint8
	enum                                 []any // só escalares
	props                                map[string]*node
	required                             []string
	extra, items, target                 *node // additionalProperties, items e o alvo do $ref
	ref                                  string
	pattern                              *regexp.Regexp
	minItems, maxItems, minLen, maxLen   *float64
	min, max, exclusiveMin, exclusiveMax *float64
	anyOf                                []*node
}

var anything = &node{} // regra das propriedades e itens sem schema: só conta a profundidade

// Schema é um InputSchema compilado, seguro para uso concorrente.
type Schema struct{ root *node }

func compile(raw []byte) (*Schema, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("validator: schema: %w", err)
	}
	c := &compiler{defs: map[string]*node{}}
	root, err := c.node(doc, "#")
	if err != nil {
		return nil, err
	}
	c.defs["#"] = root
	for _, n := range c.refs {
		if n.target = c.defs[n.ref]; n.target == nil {
			return nil, fmt.Errorf("validator: $ref %q não encontrado (só #, #/$defs/x e #/definitions/x)", n.ref)
		}
	}
	return &Schema{root: root}, nil
}

type compiler struct {
	defs map[string]*node // "#/$defs/x" -> schema
	refs []*node          // a resolver no fim
}

func (c *compiler) node(raw any, path string) (*node, error) {
	n := &node{}
	if b, ok := raw.(bool); ok {
		if !b {
			n.types = tNone
		}
		return n, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("validator: %s: schema precisa ser objeto ou booleano", path)
	}
	for _, k := range slices.Sorted(maps.Keys(m)) { // ordem fixa: o mesmo schema dá sempre o mesmo erro
		v, at := m[k], path+"/"+k
		var err error
		switch k {
		case "$schema", "$comment", "title", "description", "default", "examples", "format", "deprecated", "readOnly", "writeOnly":
		case "type":
			n.types, err = types(v)
		case "enum", "const":
			if k == "const" {
				v = []any{v}
			}
			n.enum, err = scalars(v)
		case "required":
			n.required, err = strs(v)
		case "minItems", "maxItems", "minLength", "maxLength", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum":
			*map[string]**float64{"minItems": &n.minItems, "maxItems": &n.maxItems, "minLength": &n.minLen, "maxLength": &n.maxLen, // o campo de cada limite
				"minimum": &n.min, "maximum": &n.max, "exclusiveMinimum": &n.exclusiveMin, "exclusiveMaximum": &n.exclusiveMax}[k], err = num(v)
		case "pattern":
			s, isStr := v.(string)
			if n.pattern, err = regexp.Compile(s); !isStr {
				err = errors.New("precisa ser string")
			}
		case "$ref":
			if n.ref, ok = v.(string); !ok {
				err = errors.New("precisa ser string")
			}
			c.refs = append(c.refs, n)
		case "additionalProperties", "items":
			sub, err := c.node(v, at)
			if err != nil {
				return nil, err
			}
			*map[string]**node{"items": &n.items, "additionalProperties": &n.extra}[k] = sub
		case "properties", "$defs", "definitions":
			subs, isMap := v.(map[string]any)
			if !isMap || k != "properties" && path != "#" {
				return nil, fmt.Errorf("validator: %s: precisa ser objeto (e $defs só na raiz)", at)
			}
			dst, prefix := c.defs, at+"/" // $defs e definitions: "#/$defs/x" -> schema
			if k == "properties" {
				n.props, prefix = map[string]*node{}, ""
				dst = n.props
			}
			for _, name := range slices.Sorted(maps.Keys(subs)) {
				if dst[prefix+name], err = c.node(subs[name], at+"/"+name); err != nil {
					return nil, err
				}
			}
		case "anyOf":
			list, isList := v.([]any)
			if !isList || len(list) == 0 {
				return nil, fmt.Errorf("validator: %s: precisa ser lista não vazia", at)
			}
			n.anyOf = make([]*node, len(list))
			for i, item := range list {
				if n.anyOf[i], err = c.node(item, at+"/"+strconv.Itoa(i)); err != nil {
					return nil, err
				}
			}
		default:
			err = errors.New("palavra-chave não suportada")
		}
		if err != nil {
			return nil, fmt.Errorf("validator: %s: %w", at, err)
		}
	}
	return n, nil
}

// types lê "type": um nome ou uma lista não vazia de nomes.
func types(v any) (t uint8, err error) {
	names, ok := v.([]any)
	if !ok {
		names = []any{v}
	}
	for _, name := range names {
		s, _ := name.(string)
		if t |= typeBits[s]; typeBits[s] == 0 {
			return 0, fmt.Errorf("tipo %v desconhecido", name)
		}
	}
	if t == 0 {
		err = errors.New("lista de tipos vazia")
	}
	return t, err
}

// scalars lê enum: lista não vazia de escalares (objeto e lista ficam de fora).
func scalars(v any) ([]any, error) {
	list, ok := v.([]any)
	for _, e := range list {
		switch e.(type) {
		case string, float64, bool, nil:
		default:
			ok = false
		}
	}
	if !ok || len(list) == 0 {
		return nil, errors.New("precisa ser lista não vazia de escalares")
	}
	return list, nil
}

func strs(v any) ([]string, error) {
	list, ok := v.([]any)
	out := make([]string, len(list))
	for i := 0; ok && i < len(list); i++ {
		out[i], ok = list[i].(string)
	}
	if !ok {
		return nil, errors.New("precisa ser lista de strings")
	}
	return out, nil
}

func num(v any) (*float64, error) {
	if f, ok := v.(float64); ok {
		return &f, nil
	}
	return nil, errors.New("precisa ser número")
}

// Validate confere args, o JSON não confiável de uma chamada; vazio vale {}
// (no MCP, arguments é opcional). Todo erro embrulha gwerrors.ErrInvalidPayload.
func (s *Schema) Validate(args []byte) error {
	if len(args) > maxBytes {
		return invalid("argumentos com %d bytes (máximo %d)", len(args), maxBytes)
	}
	if len(bytes.TrimSpace(args)) == 0 {
		args = []byte("{}")
	}
	var v any
	if err := json.Unmarshal(args, &v); err != nil { // também recusa dados depois do valor
		return invalid("JSON inválido: %v", err)
	}
	return s.root.check(v, "#", 0)
}

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{gwerrors.ErrInvalidPayload}, a...)...)
}

func (n *node) check(v any, path string, level int) error {
	if level >= maxDepth {
		return invalid("%s: aninhamento passa de %d (dados fundos demais ou $ref em ciclo)", path, maxDepth)
	}
	if n.target != nil {
		if err := n.target.check(v, path, level+1); err != nil {
			return err
		}
	}
	if k := kind(v); n.types != 0 && n.types&k == 0 {
		return invalid("%s: tipo %s não permitido", path, typeNames[bits.TrailingZeros8(k)])
	}
	if n.enum != nil && !slices.Contains(n.enum, v) { // enum só tem escalares: o == não entra em pânico
		return invalid("%s: valor fora do enum", path)
	}
	switch x := v.(type) {
	case map[string]any:
		for _, r := range n.required {
			if _, ok := x[r]; !ok {
				return invalid("%s: falta a propriedade %q", path, r)
			}
		}
		for key, val := range x {
			sub := n.props[key]
			if sub == nil {
				sub = cmp.Or(n.extra, anything)
			}
			if err := sub.check(val, path+"/"+key, level+1); err != nil {
				return err
			}
		}
	case []any:
		if outside(float64(len(x)), n.minItems, n.maxItems) {
			return invalid("%s: %d itens fora dos limites", path, len(x))
		}
		for i, item := range x {
			if err := cmp.Or(n.items, anything).check(item, path+"/"+strconv.Itoa(i), level+1); err != nil {
				return err
			}
		}
	case string:
		if l := utf8.RuneCountInString(x); outside(float64(l), n.minLen, n.maxLen) {
			return invalid("%s: %d caracteres fora dos limites", path, l)
		}
		if n.pattern != nil && !n.pattern.MatchString(x) {
			return invalid("%s: não casa com %s", path, n.pattern)
		}
	case float64:
		if outside(x, n.min, n.max) || n.exclusiveMin != nil && x <= *n.exclusiveMin || n.exclusiveMax != nil && x >= *n.exclusiveMax {
			return invalid("%s: %v fora dos limites", path, x)
		}
	}
	if n.anyOf != nil && !slices.ContainsFunc(n.anyOf, func(sub *node) bool { return sub.check(v, path, level+1) == nil }) {
		return invalid("%s: não casa com nenhuma alternativa de anyOf", path)
	}
	return nil
}

// outside diz se f fica abaixo de lo ou acima de hi (nil: sem limite).
func outside(f float64, lo, hi *float64) bool {
	return lo != nil && f < *lo || hi != nil && f > *hi
}

// kind devolve o bit de tipo de v.
func kind(v any) uint8 {
	switch x := v.(type) {
	case nil:
		return tNull
	case bool:
		return tBool
	case string:
		return tString
	case float64:
		if x == math.Trunc(x) {
			return tInteger
		}
		return tNumber
	case []any:
		return tArray
	}
	return tObject
}

// Cache compila cada InputSchema uma vez por ferramenta: a chave é o nome, e o
// hash do schema serializado decide se a versão guardada ainda vale (quem troca
// de schema recompila; uma entrada por ferramenta). O valor zero está pronto.
type Cache struct{ m sync.Map } // nome -> cached

type cached struct {
	sum    [sha256.Size]byte
	schema *Schema
}

// Validate confere args da chamada a tool. O schema é normalizado por
// json.Marshal (aceita tipos Go, como []string em required); o que não compila
// devolve o erro de compilação, que é do servidor, em toda chamada.
func (c *Cache) Validate(tool mcp.Tool, args json.RawMessage) error {
	raw, err := json.Marshal(tool.InputSchema())
	if err != nil {
		return fmt.Errorf("validator: schema de %s não serializa: %w", tool.Name(), err)
	}
	sum := sha256.Sum256(raw)
	e, _ := c.m.Load(tool.Name())
	got, _ := e.(cached)
	if got.schema == nil || got.sum != sum {
		if got.schema, err = compile(raw); err != nil {
			return err
		}
		got.sum = sum
		c.m.Store(tool.Name(), got)
	}
	return got.schema.Validate(args)
}
