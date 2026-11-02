package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Keep output schemas outside the Anthropic tool-schema cleaner: removing an
// unsupported constraint would silently change the client's output contract.
func applyGeminiResponseFormat(body []byte, format json.RawMessage) ([]byte, error) {
	if len(bytes.TrimSpace(format)) == 0 || bytes.Equal(bytes.TrimSpace(format), []byte("null")) {
		return body, nil
	}
	if !json.Valid(format) {
		return nil, errors.New("response_format must be valid JSON")
	}
	value := gjson.ParseBytes(format)
	if !value.IsObject() {
		return nil, errors.New("response_format must be an object")
	}
	kind := value.Get("type")
	if kind.Type != gjson.String {
		return nil, errors.New("response_format.type is required")
	}
	var schema []byte
	switch kind.Str {
	case "text":
		if err := geminiFormatFields(value, "type"); err != nil {
			return nil, err
		}
		return body, nil
	case "json_object":
		if err := geminiFormatFields(value, "type"); err != nil {
			return nil, err
		}
		schema = []byte(`{"type":"object"}`)
	case "json_schema":
		if err := geminiFormatFields(value, "type", "json_schema"); err != nil {
			return nil, err
		}
		spec := value.Get("json_schema")
		if !spec.IsObject() {
			return nil, errors.New("response_format.json_schema must be an object")
		}
		if err := geminiFormatFields(spec, "name", "description", "strict", "schema"); err != nil {
			return nil, err
		}
		if name := spec.Get("name"); name.Type != gjson.String || strings.TrimSpace(name.Str) == "" {
			return nil, errors.New("response_format.json_schema.name must be a nonempty string")
		}
		if strict := spec.Get("strict"); strict.Exists() && strict.Type != gjson.Null && strict.Type != gjson.True && strict.Type != gjson.False {
			return nil, errors.New("response_format.json_schema.strict must be boolean")
		}
		source := spec.Get("schema")
		support := geminiSchemaSupport{root: source, activeRefs: map[string]bool{}}
		if err := support.check(source, 0); err != nil {
			return nil, err
		}
		schema = []byte(source.Raw)
		if description := spec.Get("description"); description.Exists() {
			if description.Type != gjson.String {
				return nil, errors.New("response_format.json_schema.description must be a string")
			}
			text := description.Str
			if own := source.Get("description").String(); own != "" && own != text {
				if text == "" {
					text = own
				} else {
					text += "\n" + own
				}
			}
			if text != "" {
				if source.Get("$ref").Exists() {
					return nil, errors.New("response_format description beside a root $ref cannot be preserved by Gemini")
				}
				var err error
				schema, err = sjson.SetBytes(schema, "description", text)
				if err != nil {
					return nil, err
				}
			}
		}
	default:
		return nil, errors.New("response_format.type has no Gemini structured-output mapping")
	}
	result, err := sjson.SetBytes(body, "generationConfig.responseMimeType", "application/json")
	if err != nil {
		return nil, err
	}
	return sjson.SetRawBytes(result, "generationConfig.responseJsonSchema", schema)
}

func geminiFormatFields(value gjson.Result, allowed ...string) error {
	seen := map[string]bool{}
	var err error
	value.ForEach(func(k, v gjson.Result) bool {
		valid := false
		for _, name := range allowed {
			if k.Str == name {
				valid = true
				break
			}
		}
		if !valid || seen[k.Str] {
			err = errors.New("response_format contains an unsupported or duplicate field")
			return false
		}
		seen[k.Str] = true
		return true
	})
	return err
}

// Shared JSON Schema subset documented by Gemini GenerateContent and Vertex v1.
// In particular oneOf is not equivalent (Google interprets it as anyOf), and
// string length/pattern constraints must not be passed as if enforced.
type geminiSchemaSupport struct {
	root       gjson.Result
	activeRefs map[string]bool
	visits     int
}

func (s *geminiSchemaSupport) check(node gjson.Result, depth int) error {
	s.visits++
	if depth > 64 || s.visits > 10000 {
		return errors.New("response_format schema exceeds the compatibility complexity limit")
	}
	if !node.IsObject() {
		return errors.New("response_format schema nodes must be objects")
	}
	seen := map[string]bool{}
	var failure error
	node.ForEach(func(key, value gjson.Result) bool {
		if seen[key.Str] {
			failure = errors.New("response_format schema contains duplicate keywords")
			return false
		}
		seen[key.Str] = true
		valid := true
		switch key.Str {
		case "$schema":
			valid = depth == 0 && value.Type == gjson.String && geminiSchemaDialect(value.Str)
		case "title", "description", "$anchor":
			valid = value.Type == gjson.String
		case "$id":
			valid = depth == 0 && value.Type == gjson.String
		case "$ref":
			valid = value.Type == gjson.String && strings.HasPrefix(value.Str, "#/")
			if valid {
				target := s.root
				for _, part := range strings.Split(strings.TrimPrefix(value.Str, "#/"), "/") {
					part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
					if !target.IsObject() {
						valid = false
						break
					}
					target = target.Map()[part]
				}
				if s.activeRefs[value.Str] {
					failure = errors.New("recursive response_format schema references cannot be preserved by Gemini")
					return false
				}
				if valid {
					s.activeRefs[value.Str] = true
					failure = s.check(target, depth+1)
					delete(s.activeRefs, value.Str)
				}
			}
		case "type":
			types := []gjson.Result{value}
			if value.IsArray() {
				types = value.Array()
			}
			valid = len(types) > 0
			for _, t := range types {
				valid = valid && t.Type == gjson.String && geminiSchemaType(t.Str)
			}
		case "format":
			valid = value.Type == gjson.String && (value.Str == "date-time" || value.Str == "date" || value.Str == "time")
		case "properties", "$defs":
			valid = value.IsObject()
			names := map[string]bool{}
			if valid {
				value.ForEach(func(k, v gjson.Result) bool {
					if names[k.Str] {
						failure = errors.New("response_format schema contains duplicate names")
						return false
					}
					names[k.Str] = true
					failure = s.check(v, depth+1)
					return failure == nil
				})
			}
		case "items":
			failure = s.check(value, depth+1)
		case "additionalProperties":
			if value.Type != gjson.True && value.Type != gjson.False {
				failure = s.check(value, depth+1)
			}
		case "anyOf", "prefixItems":
			valid = value.IsArray() && len(value.Array()) > 0
			if key.Str == "prefixItems" {
				dialect := s.root.Get("$schema").String()
				valid = valid && (dialect == "" || strings.Contains(dialect, "/2020-12/"))
			}
			if valid {
				for _, v := range value.Array() {
					if failure = s.check(v, depth+1); failure != nil {
						break
					}
				}
			}
		case "required", "propertyOrdering":
			valid = value.IsArray()
			for _, v := range value.Array() {
				valid = valid && v.Type == gjson.String
			}
		case "enum":
			valid = value.IsArray() && len(value.Array()) > 0
			for _, v := range value.Array() {
				valid = valid && (v.Type == gjson.String || v.Type == gjson.Number)
			}
		case "minimum", "maximum":
			valid = value.Type == gjson.Number
		case "minItems", "maxItems":
			valid = value.Type == gjson.Number && value.Num >= 0 && value.Num == math.Trunc(value.Num)
		default:
			failure = errors.New("response_format schema contains a constraint Gemini cannot enforce; unsupported constraints are not removed")
		}
		if !valid && failure == nil {
			failure = errors.New("response_format schema contains an unsupported keyword value")
		}
		return failure == nil
	})
	if failure != nil {
		return failure
	}
	if _, ref := seen["$ref"]; ref {
		for name := range seen {
			if !strings.HasPrefix(name, "$") {
				return errors.New("response_format schema $ref siblings cannot be preserved by Gemini")
			}
		}
	}
	return nil
}

func geminiSchemaType(value string) bool {
	switch value {
	case "object", "array", "string", "number", "integer", "boolean", "null":
		return true
	default:
		return false
	}
}

// These dialects share the accepted constraints. prefixItems additionally
// requires the 2020-12 dialect; older declarations must not change its meaning.
func geminiSchemaDialect(value string) bool {
	value = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(value, "https://"), "http://"), "#")
	switch value {
	case "json-schema.org/draft/2020-12/schema", "json-schema.org/draft/2019-09/schema", "json-schema.org/draft-07/schema":
		return true
	default:
		return false
	}
}
