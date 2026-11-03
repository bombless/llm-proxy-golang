package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	ID              string  `json:"id"`
	PublicModel     string  `json:"public_model"`
	URL             string  `json:"url"`
	Key             string  `json:"key"`
	UpstreamModel   string  `json:"upstream_model"`
	UseProxy        bool    `json:"use_proxy"`
	ProxyFromChat   bool    `json:"proxy_from_chat_completions"`
	CachePrice      float64 `json:"cache_price"`
	PrefillPrice    float64 `json:"prefill_price"`
	GenerationPrice float64 `json:"generation_price"`
	Enabled         *bool   `json:"enabled"`
	Hidden          bool    `json:"hidden"`
}
type Config struct {
	Chat      []Entry `json:"chat_completions"`
	Responses []Entry `json:"responses"`
}
type Usage struct {
	ID             string  `json:"id"`
	At             string  `json:"at"`
	Type           string  `json:"type"`
	ConfigID       string  `json:"config_id"`
	PublicModel    string  `json:"public_model"`
	UpstreamModel  string  `json:"upstream_model"`
	Status         int     `json:"status"`
	Input          any     `json:"input_tokens"`
	Cached         any     `json:"cached_tokens"`
	Output         any     `json:"output_tokens"`
	CacheCost      float64 `json:"cache_cost"`
	PrefillCost    float64 `json:"prefill_cost"`
	GenerationCost float64 `json:"generation_cost"`
	TotalCost      float64 `json:"total_cost"`
	Duration       int64   `json:"duration_ms"`
}
type App struct {
	mu                                                       sync.RWMutex
	cfg                                                      Config
	configPath, usagePath, metricsPath, countPath, statePath string
	usage                                                    []Usage
	metrics                                                  map[string][]map[string]any
	counts                                                   map[string]int
	sessions                                                 map[string]any
	responses                                                map[string]any
	client                                                   *http.Client
	proxyURL                                                 string
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func readJSON(path string, dst any) {
	b, e := os.ReadFile(path)
	if e == nil {
		_ = json.Unmarshal(b, dst)
	}
}
func atomicJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, b, 0600); e != nil {
		return e
	}
	return os.Rename(tmp, path)
}
func id() string { b := make([]byte, 12); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func enabled(e Entry) bool {
	return (e.Enabled == nil || *e.Enabled) && !e.Hidden && e.PublicModel != ""
}
func (a *App) saveUsage() { _ = atomicJSON(a.usagePath, a.usage) }
func (a *App) addUsage(typ string, e Entry, start time.Time, status int, body []byte) {
	var x map[string]any
	_ = json.Unmarshal(body, &x)
	u, _ := x["usage"].(map[string]any)
	num := func(keys ...string) float64 {
		for _, k := range keys {
			if v, ok := u[k].(float64); ok {
				return v
			}
		}
		return 0
	}
	in := num("prompt_tokens", "input_tokens")
	cached := num("cached_tokens")
	if d, ok := u["prompt_tokens_details"].(map[string]any); ok {
		if v, ok := d["cached_tokens"].(float64); ok {
			cached = v
		}
	}
	if d, ok := u["input_tokens_details"].(map[string]any); ok {
		if v, ok := d["cached_tokens"].(float64); ok {
			cached = v
		}
	}
	out := num("completion_tokens", "output_tokens")
	cc := cached * e.CachePrice / 1e6
	pc := (in - cached) * e.PrefillPrice / 1e6
	gc := out * e.GenerationPrice / 1e6
	a.mu.Lock()
	a.usage = append(a.usage, Usage{ID: id(), At: time.Now().Format(time.RFC3339Nano), Type: typ, ConfigID: e.ID, PublicModel: e.PublicModel, UpstreamModel: first(e.UpstreamModel, e.PublicModel), Status: status, Input: in, Cached: cached, Output: out, CacheCost: cc, PrefillCost: pc, GenerationCost: gc, TotalCost: cc + pc + gc, Duration: time.Since(start).Milliseconds()})
	_ = atomicJSON(a.usagePath, a.usage)
	a.mu.Unlock()
}
func first(x, y string) string {
	if x != "" {
		return x
	}
	return y
}
func (a *App) entry(typ, model string) (Entry, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	rows := a.cfg.Chat
	if typ == "responses" {
		rows = a.cfg.Responses
	}
	for _, e := range rows {
		if e.PublicModel == model && enabled(e) {
			return e, true
		}
	}
	return Entry{}, false
}
func (a *App) target(e Entry, typ string, forceChat bool) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(e.URL))
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("invalid upstream URL")
	}
	if u.Path == "" || u.Path == "/" {
		if forceChat || typ == "chat_completions" {
			u.Path = "/v1/chat/completions"
		} else {
			u.Path = "/v1/responses"
		}
	}
	if forceChat && strings.HasSuffix(strings.TrimRight(u.Path, "/"), "/responses") {
		u.Path = strings.TrimSuffix(strings.TrimRight(u.Path, "/"), "/responses") + "/chat/completions"
	}
	return u, nil
}
func (a *App) forward(w http.ResponseWriter, r *http.Request, e Entry, typ string, body []byte, target *url.URL) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(mustInt(env("UPSTREAM_TIMEOUT", "120")))*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		writeErr(w, 502, err.Error(), "proxy_error")
		return
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", env("UPSTREAM_USER_AGENT", "curl/8.0"))
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if e.Key != "" {
		req.Header.Set("Authorization", "Bearer "+e.Key)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		writeErr(w, 502, err.Error(), "proxy_error")
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		lk := strings.ToLower(k)
		if lk == "connection" || lk == "transfer-encoding" || lk == "content-length" || lk == "keep-alive" {
			continue
		}
		for _, x := range v {
			w.Header().Add(k, x)
		}
	}
	w.WriteHeader(resp.StatusCode)
	data, er := io.ReadAll(resp.Body)
	if er == nil {
		_, _ = w.Write(data)
		a.addUsage(typ, e, time.Now().Add(-1*time.Millisecond), resp.StatusCode, data)
	}
}
func mustInt(s string) int {
	n, e := strconv.Atoi(s)
	if e != nil || n < 1 {
		return 120
	}
	return n
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeErr(w http.ResponseWriter, status int, msg, typ string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"message": msg, "type": typ}})
}
func decodeBody(r *http.Request) (map[string]any, []byte, error) {
	b, e := io.ReadAll(http.MaxBytesReader(nil, r.Body, 50<<20))
	if e != nil {
		return nil, nil, e
	}
	var m map[string]any
	if len(b) > 0 {
		e = json.Unmarshal(b, &m)
		if e != nil {
			return nil, nil, e
		}
	}
	return m, b, nil
}
func (a *App) proxy(w http.ResponseWriter, r *http.Request, typ string) {
	body, raw, err := decodeBody(r)
	if err != nil {
		writeErr(w, 400, err.Error(), "invalid_request_error")
		return
	}
	model, _ := body["model"].(string)
	e, ok := a.entry(typ, model)
	if !ok {
		writeErr(w, 404, "No upstream configured for model: "+first(model, "(missing)"), "model_not_configured")
		return
	}
	force := typ == "responses" && e.ProxyFromChat
	target, err := a.target(e, typ, force)
	if err != nil {
		writeErr(w, 500, err.Error(), "proxy_config_error")
		return
	}
	if force {
		converted, err := responsesToChat(body)
		if err != nil {
			writeErr(w, 400, err.Error(), "invalid_request_error")
			return
		}
		converted["model"] = first(e.UpstreamModel, model)
		raw, _ = json.Marshal(converted)
	} else if len(body) > 0 {
		body["model"] = first(e.UpstreamModel, model)
		raw, _ = json.Marshal(body)
	}
	a.forward(w, r, e, typ, raw, target)
}
func responsesToChat(b map[string]any) (map[string]any, error) {
	msgs := []any{}
	if ins, ok := b["instructions"]; ok {
		msgs = append(msgs, map[string]any{"role": "system", "content": ins})
	}
	switch in := b["input"].(type) {
	case string:
		msgs = append(msgs, map[string]any{"role": "user", "content": in})
	case []any:
		for _, v := range in {
			m, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if m["type"] == "function_call_output" {
				msgs = append(msgs, map[string]any{"role": "tool", "tool_call_id": m["call_id"], "content": fmt.Sprint(m["output"])})
				continue
			}
			role := first(fmt.Sprint(m["role"]), "user")
			content := m["content"]
			if content == nil {
				content = m["text"]
			}
			if content != nil {
				msgs = append(msgs, map[string]any{"role": role, "content": content})
			}
		}
	}
	out := map[string]any{"model": b["model"], "messages": msgs}
	for _, k := range []string{"temperature", "top_p", "stream", "stop", "presence_penalty", "frequency_penalty", "seed", "response_format", "reasoning_effort", "tool_choice", "tools"} {
		if v, ok := b[k]; ok {
			out[k] = v
		}
	}
	if v, ok := b["max_output_tokens"]; ok {
		out["max_completion_tokens"] = v
	}
	return out, nil
}
func (a *App) configGet(w http.ResponseWriter) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	clone := func(rows []Entry) []map[string]any {
		out := make([]map[string]any, 0, len(rows))
		for _, e := range rows {
			b, _ := json.Marshal(e)
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			if e.Key != "" {
				m["key"] = "********"
			} else {
				m["key"] = ""
			}
			out = append(out, m)
		}
		return out
	}
	writeJSON(w, 200, map[string]any{"chat_completions": clone(a.cfg.Chat), "responses": clone(a.cfg.Responses)})
}
func (a *App) configPut(w http.ResponseWriter, r *http.Request) {
	var incoming map[string][]map[string]any
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 5<<20)).Decode(&incoming) != nil {
		writeErr(w, 400, "invalid config", "invalid_request_error")
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	convert := func(key string, old []Entry) []Entry {
		rows, ok := incoming[key]
		if !ok {
			return old
		}
		out := []Entry{}
		for _, m := range rows {
			get := func(k string) string { v, _ := m[k].(string); return strings.TrimSpace(v) }
			en := Entry{ID: get("id"), PublicModel: get("public_model"), URL: get("url"), Key: get("key"), UpstreamModel: get("upstream_model"), UseProxy: m["use_proxy"] != false, ProxyFromChat: m["proxy_from_chat_completions"] == true, Enabled: boolPtr(m["enabled"] != false), Hidden: m["hidden"] == true}
			if en.Key == "********" {
				for _, o := range old {
					if o.ID == en.ID {
						en.Key = o.Key
					}
				}
			}
			for k, p := range map[string]*float64{"cache_price": &en.CachePrice, "prefill_price": &en.PrefillPrice, "generation_price": &en.GenerationPrice} {
				if v, ok := m[k].(float64); ok {
					*p = v
				}
			}
			if en.ID == "" {
				en.ID = id()
			}
			if en.PublicModel != "" && en.URL != "" {
				out = append(out, en)
			}
		}
		return out
	}
	a.cfg.Chat = convert("chat_completions", a.cfg.Chat)
	a.cfg.Responses = convert("responses", a.cfg.Responses)
	if err := atomicJSON(a.configPath, a.cfg); err != nil {
		writeErr(w, 500, err.Error(), "storage_error")
		return
	}
	a.configGetLocked(w)
}
func boolPtr(v bool) *bool { return &v }
func (a *App) configGetLocked(w http.ResponseWriter) {
	clone := func(rows []Entry) []map[string]any {
		out := []map[string]any{}
		for _, e := range rows {
			b, _ := json.Marshal(e)
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			if e.Key != "" {
				m["key"] = "********"
			} else {
				m["key"] = ""
			}
			out = append(out, m)
		}
		return out
	}
	writeJSON(w, 200, map[string]any{"chat_completions": clone(a.cfg.Chat), "responses": clone(a.cfg.Responses)})
}
func (a *App) models(w http.ResponseWriter) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	seen := map[string]bool{}
	out := []any{}
	for _, rows := range [][]Entry{a.cfg.Chat, a.cfg.Responses} {
		for _, e := range rows {
			if enabled(e) && !seen[e.PublicModel] {
				seen[e.PublicModel] = true
				out = append(out, map[string]any{"id": e.PublicModel, "object": "model", "created": 0, "owned_by": "llm-proxy"})
			}
		}
	}
	writeJSON(w, 200, map[string]any{"object": "list", "data": out})
}
func (a *App) usageGet(w http.ResponseWriter) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	groups := map[string]map[string]any{}
	for _, u := range a.usage {
		k := u.Type + ":" + u.ConfigID
		g := groups[k]
		if g == nil {
			g = map[string]any{"type": u.Type, "config_id": u.ConfigID, "public_model": u.PublicModel, "calls": 0, "total_cost": 0.0, "input_tokens": 0.0, "cached_tokens": 0.0, "output_tokens": 0.0}
			groups[k] = g
		}
		g["calls"] = g["calls"].(int) + 1
		g["total_cost"] = g["total_cost"].(float64) + u.TotalCost
	}
	summary := []any{}
	for _, v := range groups {
		summary = append(summary, v)
	}
	sort.Slice(summary, func(i, j int) bool {
		return summary[i].(map[string]any)["total_cost"].(float64) > summary[j].(map[string]any)["total_cost"].(float64)
	})
	start := len(a.usage) - 200
	if start < 0 {
		start = 0
	}
	recent := append([]Usage(nil), a.usage[start:]...)
	for i, j := 0, len(recent)-1; i < j; i, j = i+1, j-1 {
		recent[i], recent[j] = recent[j], recent[i]
	}
	writeJSON(w, 200, map[string]any{"summary": summary, "recent": recent})
}
func (a *App) metricsGet(w http.ResponseWriter) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := map[string]any{}
	for typ, rows := range map[string][]Entry{"chat_completions": a.cfg.Chat, "responses": a.cfg.Responses} {
		list := []any{}
		for _, e := range rows {
			k := typ + ":" + e.ID
			list = append(list, map[string]any{"id": e.ID, "public_model": e.PublicModel, "upstream_model": first(e.UpstreamModel, e.PublicModel), "count": a.counts[k], "samples": a.metrics[k]})
		}
		out[typ] = list
	}
	writeJSON(w, 200, out)
}
func (a *App) sessionsGet(w http.ResponseWriter) {
	b, e := os.ReadFile(a.statePath)
	if e == nil {
		var v map[string]any
		if json.Unmarshal(b, &v) == nil {
			writeJSON(w, 200, []any{})
			return
		}
	}
	writeJSON(w, 200, []any{})
}
func (a *App) errorStats(w http.ResponseWriter, r *http.Request, kind string) {
	rangeQ := r.URL.Query().Get("range")
	hours := 24
	if rangeQ == "4h" {
		hours = 4
	} else if rangeQ == "12h" {
		hours = 12
	} else if rangeQ == "72h" {
		hours = 72
	} else if rangeQ == "7d" {
		hours = 168
	}
	if kind == "timeline" {
		if n, e := strconv.Atoi(r.URL.Query().Get("hours")); e == nil && n > 0 {
			hours = n
		}
		if hours > 168 {
			hours = 168
		}
	}
	cut := time.Now().Add(-time.Duration(hours) * time.Hour)
	total, errs := 0, 0
	by := map[string]map[string]int{}
	for _, u := range a.usage {
		t, e := time.Parse(time.RFC3339Nano, u.At)
		if e != nil || t.Before(cut) {
			continue
		}
		total++
		if u.Status < 200 || u.Status >= 400 {
			errs++
			g := by[u.PublicModel]
			if g == nil {
				g = map[string]int{"total": 0, "errors": 0}
				by[u.PublicModel] = g
			}
			g["errors"]++
		}
		g := by[u.PublicModel]
		if g == nil {
			g = map[string]int{"total": 0, "errors": 0}
			by[u.PublicModel] = g
		}
		g["total"]++
	}
	rate := 0.0
	if total > 0 {
		rate = float64(errs) * 100 / float64(total)
	}
	switch kind {
	case "summary":
		writeJSON(w, 200, map[string]any{"total": total, "errors": errs, "rate": rate})
	case "models":
		arr := []any{}
		for m, g := range by {
			rr := 0.0
			if g["total"] > 0 {
				rr = float64(g["errors"]) * 100 / float64(g["total"])
			}
			arr = append(arr, map[string]any{"model": m, "total": g["total"], "errors": g["errors"], "rate": rr})
		}
		writeJSON(w, 200, map[string]any{"models": arr})
	default:
		writeJSON(w, 200, map[string]any{"hours": hours, "points": []any{}})
	}
}
func (a *App) testEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, 405, "method not allowed", "invalid_request_error")
		return
	}
	var q struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&q) != nil || (q.Type != "chat_completions" && q.Type != "responses") || q.ID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "Invalid test request"})
		return
	}
	a.mu.RLock()
	rows := a.cfg.Chat
	if q.Type == "responses" {
		rows = a.cfg.Responses
	}
	var e Entry
	found := false
	for _, v := range rows {
		if v.ID == q.ID {
			e = v
			found = true
			break
		}
	}
	a.mu.RUnlock()
	if !found {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "Upstream not found"})
		return
	}
	model := first(e.UpstreamModel, e.PublicModel)
	var payload any
	if q.Type == "responses" && e.ProxyFromChat {
		payload = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "你好"}}}
	} else if q.Type == "responses" {
		payload = map[string]any{"model": model, "input": "你好"}
	} else {
		payload = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "你好"}}}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	target, err := a.target(e, q.Type, q.Type == "responses" && e.ProxyFromChat)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(mustInt(env("UPSTREAM_TIMEOUT", "120")))*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		writeJSON(w, 502, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", env("UPSTREAM_USER_AGENT", "curl/8.0"))
	if e.Key != "" {
		req.Header.Set("Authorization", "Bearer "+e.Key)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		writeJSON(w, 502, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 20<<20))
	if err != nil {
		writeJSON(w, 502, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	textOut := extractText(raw, q.Type)
	writeJSON(w, 200, map[string]any{"ok": true, "status": resp.StatusCode, "text": textOut, "raw": string(raw)})
}
func extractText(raw []byte, typ string) string {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return string(raw)
	}
	if typ == "responses" {
		if s, ok := m["output_text"].(string); ok && s != "" {
			return s
		}
		if arr, ok := m["output"].([]any); ok {
			var b strings.Builder
			for _, v := range arr {
				if obj, ok := v.(map[string]any); ok {
					if parts, ok := obj["content"].([]any); ok {
						for _, p := range parts {
							if part, ok := p.(map[string]any); ok && part["type"] == "output_text" {
								b.WriteString(fmt.Sprint(part["text"]))
							}
						}
					}
				}
			}
			if b.Len() > 0 {
				return b.String()
			}
		}
	}
	if choices, ok := m["choices"].([]any); ok && len(choices) > 0 {
		if c, ok := choices[0].(map[string]any); ok {
			if msg, ok := c["message"].(map[string]any); ok {
				switch v := msg["content"].(type) {
				case string:
					return v
				case []any:
					var b strings.Builder
					for _, p := range v {
						if part, ok := p.(map[string]any); ok {
							b.WriteString(fmt.Sprint(part["text"]))
						}
					}
					return b.String()
				}
			}
			if s, ok := c["text"].(string); ok {
				return s
			}
		}
	}
	if e, ok := m["error"].(map[string]any); ok {
		return fmt.Sprint(e["message"])
	}
	return string(raw)
}

func (a *App) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/test", a.testEndpoint)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]any{"ok": true}) })
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) { a.models(w) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) { a.proxy(w, r, "chat_completions") })
	mux.HandleFunc("/v1/responses", func(w http.ResponseWriter, r *http.Request) { a.proxy(w, r, "responses") })
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			a.configPut(w, r)
		} else {
			a.configGet(w)
		}
	})
	mux.HandleFunc("/api/usage", func(w http.ResponseWriter, r *http.Request) { a.usageGet(w) })
	mux.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) { a.metricsGet(w) })
	mux.HandleFunc("/api/response-sessions", func(w http.ResponseWriter, r *http.Request) { a.sessionsGet(w) })
	mux.HandleFunc("/api/error-stats/summary", func(w http.ResponseWriter, r *http.Request) { a.errorStats(w, r, "summary") })
	mux.HandleFunc("/api/error-stats/models", func(w http.ResponseWriter, r *http.Request) { a.errorStats(w, r, "models") })
	mux.HandleFunc("/api/error-stats/timeline", func(w http.ResponseWriter, r *http.Request) { a.errorStats(w, r, "timeline") })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.FileServer(http.Dir("dist")).ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, "dist/index.html")
	})
	return mux
}
func main() {
	root, _ := filepath.Abs(".")
	a := &App{configPath: env("CONFIG_FILE", filepath.Join(root, "config.json")), usagePath: env("USAGE_FILE", filepath.Join(root, "llm-usage.json")), metricsPath: env("METRICS_FILE", filepath.Join(root, "llm-metrics.json")), countPath: env("METRICS_COUNT_FILE", filepath.Join(root, "llm-metrics-counts.json")), statePath: env("RESPONSE_STATE_FILE", filepath.Join(root, "responses-state.json")), metrics: map[string][]map[string]any{}, counts: map[string]int{}, sessions: map[string]any{}, responses: map[string]any{}, proxyURL: os.Getenv("SOCKS5_PROXY")}
	readJSON(a.configPath, &a.cfg)
	readJSON(a.usagePath, &a.usage)
	readJSON(a.metricsPath, &a.metrics)
	readJSON(a.countPath, &a.counts)
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: false, MaxIdleConns: 64, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second}
	if a.proxyURL != "" {
		if pu, e := url.Parse(a.proxyURL); e == nil {
			tr.Proxy = http.ProxyURL(pu)
		}
	}
	a.client = &http.Client{Transport: tr}
	addr := env("LISTEN_HOST", "127.0.0.1") + ":" + env("LISTEN_PORT", "18080")
	srv := &http.Server{Addr: addr, Handler: a.handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 1 << 20}
	log.Printf("llm-proxy-go listening on http://%s", addr)
	log.Fatal(srv.ListenAndServe())
}
