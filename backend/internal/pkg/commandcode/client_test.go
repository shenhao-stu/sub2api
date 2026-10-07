package commandcode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type testDoer func(*http.Request) (*http.Response, error)

func (fn testDoer) Do(r *http.Request) (*http.Response, error) { return fn(r) }

const testInput = `{"model":"test/model","messages":[{"role":"user","content":"ping"}]}`
const testFinish = `{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":100,"outputTokens":10,"inputTokenDetails":{"cacheReadTokens":60,"cacheWriteTokens":20},"outputTokenDetails":{"reasoningTokens":3}}}`

func fixtureResponse(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(body))}
}

func TestChatJSONAndSSEPreserveUsage(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) {
				if r.URL.String() != Endpoint || r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer secret" {
					t.Error("wrong credential destination")
				}
				payload, _ := io.ReadAll(r.Body)
				var body map[string]any
				if json.Unmarshal(payload, &body) != nil {
					t.Fatal("bad outgoing body")
				}
				params, ok := body["params"].(map[string]any)
				if !ok {
					t.Fatal("missing inference parameters")
				}
				if params["max_tokens"] != float64(4096) || params["model"] != "test/model" {
					t.Error("wrong inference parameters")
				}
				return fixtureResponse(r, 200, "data: {\"type\":\"text-delta\",\"text\":\" hello \"}\n\ndata: {\"type\":\"reasoning-delta\",\"text\":\" reason \"}\n\ndata: "+testFinish+"\n\ndata: [DONE]\n\n"), nil
			})}
			resp, err := client.ChatCompletion(context.Background(), "secret", []byte(testInput), stream)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{`"content":" hello "`, `"reasoning_content":" reason "`, `"prompt_tokens":100`, `"cached_tokens":60`, `"cache_creation_input_tokens":20`, `"reasoning_tokens":3`} {
				if !strings.Contains(string(data), want) {
					t.Errorf("missing %s in %s", want, data)
				}
			}
			if stream != strings.Contains(string(data), "data: [DONE]") {
				t.Error("incorrect stream termination")
			}
		})
	}
}

func TestNoSecretsInErrorsAndStatuses(t *testing.T) {
	for _, status := range []int{301, 307, 401, 403, 429, 500} {
		client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) {
			resp := fixtureResponse(r, status, "secret-account-key")
			resp.Header.Set("Retry-After", "9")
			resp.Header.Set("Set-Cookie", "secret-cookie")
			return resp, nil
		})}
		resp, err := client.ChatCompletion(context.Background(), "secret-account-key", []byte(testInput), false)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := status
		if status < 400 {
			want = 502
		}
		if resp.StatusCode != want || resp.Header.Get("Retry-After") != "9" || resp.Header.Get("Set-Cookie") != "" || strings.Contains(string(data), "secret-") {
			t.Fatalf("unsafe or lost status: %d %s", resp.StatusCode, data)
		}
	}
	client := Client{HTTPClient: testDoer(func(*http.Request) (*http.Response, error) { return nil, errors.New("secret-proxy-password") })}
	_, err := client.ChatCompletion(context.Background(), "secret", []byte(testInput), false)
	if !errors.Is(err, ErrTransport) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe transport error: %v", err)
	}
}

func TestMalformedAndIncompleteStreamsNeverComplete(t *testing.T) {
	cases := map[string]string{
		"missing_finish":      `{"type":"text-delta","text":"partial"}`,
		"done_without_finish": "data: [DONE]\n",
		"malformed_json":      "{bad}\n" + testFinish,
		"unknown_event":       `{"type":"unexpected","payload":"important"}` + "\n" + testFinish,
		"error_event":         `{"type":"error","message":"secret-account-key"}`,
		"unknown_finish":      `{"type":"finish","finishReason":"network_error","usage":{"inputTokens":1,"outputTokens":2}}`,
		"missing_usage":       `{"type":"finish","finishReason":"stop"}`,
		"event_after_finish":  testFinish + "\n" + `{"type":"error","message":"secret"}`,
	}
	for name, body := range cases {
		for _, stream := range []bool{false, true} {
			t.Run(name+fmt.Sprint(stream), func(t *testing.T) {
				client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) { return fixtureResponse(r, 200, body), nil })}
				resp, err := client.ChatCompletion(context.Background(), "secret-account-key", []byte(testInput), stream)
				var data []byte
				if err == nil {
					data, err = io.ReadAll(resp.Body)
					resp.Body.Close()
				}
				if err == nil || strings.Contains(string(data), "[DONE]") || strings.Contains(string(data), `"finish_reason":"stop"`) {
					t.Fatalf("failure reported success: %s, %v", data, err)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatal("secret in error")
				}
				if name == "event_after_finish" {
					var usageErr *UsageError
					if !errors.As(err, &usageErr) || usageErr.Usage["prompt_tokens"] != int64(100) {
						t.Fatalf("lost observed usage: %v", err)
					}
					if stream && !strings.Contains(string(data), `"cached_tokens":60`) {
						t.Error("stream did not retain observed usage")
					}
				}
			})
		}
	}
}

func TestUnsupportedRequestsFailBeforeNetwork(t *testing.T) {
	for _, input := range []string{
		`{"model":"m","messages":[{"role":"user","content":"x"}],"stop":["END"]}`,
		`{"model":"m","messages":[{"role":"user","content":"x"}],"n":2}`,
		`{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://localhost/secret"}}]}]}`,
		`{"model":"m","messages":[{"role":"tool","tool_call_id":"unknown","content":"x"}]}`,
		`{"model":"m","messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"bad"}}]}]}`,
		`{"model":"m","messages":[{"role":"user","content":"x"}],"tool_choice":"required"}`,
		`{"model":"m","messages":[{"role":"user","content":"x"}],"max_tokens":-1}`,
		`{"model":"m","messages":[{"role":"user","content":"x"}],"stream":true}`,
	} {
		client := Client{HTTPClient: testDoer(func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid request reached network")
			return nil, nil
		})}
		if _, err := client.ChatCompletion(context.Background(), "secret", []byte(input), false); !errors.Is(err, ErrRequest) {
			t.Fatalf("accepted invalid request: %s %v", input, err)
		}
	}
}

func TestToolsRoundTripAndChoices(t *testing.T) {
	input := `{"model":"m","messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"auto","parameters":{"type":"object"}}}],"tool_choice":{"type":"function","function":{"name":"auto"}}}`
	client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) {
		data, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(data), `"tool_choice":{"name":"auto","type":"tool"}`) {
			t.Errorf("lost named choice: %s", data)
		}
		return fixtureResponse(r, 200, `{"type":"tool-call","toolCallId":"c1","toolName":"auto","input":{"x":"a"}}`+"\n"+`{"type":"finish","finishReason":"tool-calls","usage":{"inputTokens":3,"outputTokens":2}}`), nil
	})}
	resp, err := client.ChatCompletion(context.Background(), "secret", []byte(input), false)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(data), `"id":"c1"`) || !strings.Contains(string(data), `"finish_reason":"tool_calls"`) {
		t.Fatalf("lost tool call: %s", data)
	}
	continuation := `{"model":"m","messages":[{"role":"user","content":"x"},{"role":"assistant","content":null,"reasoning_content":" reason ","tool_calls":[{"id":"c1","type":"function","function":{"name":"auto","arguments":"{\"x\":\"a\"}"}}]},{"role":"tool","tool_call_id":"c1","content":"tool result"}],"tool_choice":"none"}`
	prepared, err := prepareRequest([]byte(continuation), false, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"toolCallId":"c1"`, `"toolName":"auto"`, `"value":"tool result"`, `"type":"reasoning"`, `"tools":[]`} {
		if !strings.Contains(string(prepared.body), want) {
			t.Errorf("lost continuation %s", want)
		}
	}
}

func TestUsageVariantsAndInvalidNumbers(t *testing.T) {
	for _, raw := range []string{
		`{"inputTokens":{"total":10,"cacheRead":4,"cacheWrite":2},"outputTokens":{"total":3,"reasoning":1}}`,
		`{"input_tokens":10,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":2,"reasoningTokens":1}`,
		`{"prompt_tokens":10,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":4,"cache_creation_tokens":2},"completion_tokens_details":{"reasoning_tokens":1}}`,
	} {
		usage, err := convertUsage([]byte(raw))
		details, ok := usage["prompt_tokens_details"].(map[string]any)
		if err != nil || !ok || usage["prompt_tokens"] != int64(10) || details["cached_tokens"] != int64(4) {
			t.Fatalf("bad normalized usage: %#v %v", usage, err)
		}
	}
	for _, raw := range []string{
		`{"inputTokens":-1,"outputTokens":3}`, `{"inputTokens":1.5,"outputTokens":3}`, `{"inputTokens":1,"outputTokens":3,"cachedInputTokens":2}`,
		`{"inputTokens":10,"prompt_tokens":11,"outputTokens":3}`, `{"inputTokens":10,"outputTokens":3,"totalTokens":20}`, `{"inputTokens":10}`, `{"inputTokens":"10","outputTokens":3}`,
	} {
		if _, err := convertUsage([]byte(raw)); !errors.Is(err, ErrUsage) {
			t.Errorf("invalid usage accepted: %s", raw)
		}
	}
}

func TestCancellationAndCloseStopProducer(t *testing.T) {
	for _, closeBody := range []bool{false, true} {
		t.Run(fmt.Sprint(closeBody), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closed := make(chan struct{})
			client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) {
				reader, writer := io.Pipe()
				go func() { <-r.Context().Done(); _ = writer.CloseWithError(r.Context().Err()); close(closed) }()
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: reader, Request: r}, nil
			})}
			resp, err := client.ChatCompletion(ctx, "secret", []byte(testInput), true)
			if err != nil {
				t.Fatal(err)
			}
			if closeBody {
				resp.Body.Close()
			} else {
				cancel()
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("producer not canceled")
			}
			resp.Body.Close()
		})
	}
}

func TestIndependentConcurrentCredentials(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		seen[r.Header.Get("Authorization")]++
		mu.Unlock()
		return fixtureResponse(r, 200, testFinish), nil
	})}
	var wg sync.WaitGroup
	for _, key := range []string{"account-a", "account-b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			resp, err := client.ChatCompletion(context.Background(), key, []byte(testInput), false)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
		}(key)
	}
	wg.Wait()
	if seen["Bearer account-a"] != 1 || seen["Bearer account-b"] != 1 || len(seen) != 2 {
		t.Fatalf("credential bleed: %#v", seen)
	}
}

func TestEventSizeBound(t *testing.T) {
	client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) {
		return fixtureResponse(r, 200, `{"type":"text-delta","text":"`+strings.Repeat("x", MaxEventBytes)+`"}`), nil
	})}
	if _, err := client.ChatCompletion(context.Background(), "secret", []byte(testInput), false); !errors.Is(err, ErrLimit) {
		t.Fatalf("event bound not enforced: %v", err)
	}
}

func TestToolInputMustCompleteAndRespectParallelChoice(t *testing.T) {
	input := `{"model":"m","messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"lookup"}}],"parallel_tool_calls":false}`
	call := `{"type":"tool-call","toolCallId":"c1","toolName":"lookup","input":{}}`
	finish := `{"type":"finish","finishReason":"tool_calls","usage":{"inputTokens":3,"outputTokens":2}}`
	for name, body := range map[string]string{
		"unterminated":      `{"type":"tool-input-start","id":"c1","toolName":"lookup"}` + "\n" + finish,
		"orphan_delta":      `{"type":"tool-input-delta","id":"c1","delta":"{"}` + "\n" + call + "\n" + finish,
		"parallel_disabled": call + "\n" + strings.ReplaceAll(call, "c1", "c2") + "\n" + finish,
	} {
		t.Run(name, func(t *testing.T) {
			client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) { return fixtureResponse(r, 200, body), nil })}
			if _, err := client.ChatCompletion(context.Background(), "secret", []byte(input), false); !errors.Is(err, ErrProtocol) {
				t.Fatalf("invalid tool sequence accepted: %v", err)
			}
		})
	}
	client := Client{HTTPClient: testDoer(func(r *http.Request) (*http.Response, error) {
		body := `{"type":"tool-input-start","id":"c1","toolName":"lookup"}` + "\n" + `{"type":"tool-input-delta","id":"c1","delta":"{}"}` + "\n" + `{"type":"tool-input-end","id":"c1"}` + "\n" + call + "\n" + finish
		return fixtureResponse(r, 200, body), nil
	})}
	resp, err := client.ChatCompletion(context.Background(), "secret", []byte(input), false)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}
