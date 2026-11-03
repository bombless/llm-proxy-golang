# llm-proxy-golang

A lightweight Go implementation/prototype of the `llm-proxy` HTTP API.

## Build and run

```bash
go build -o llm-proxy ./cmd/llm-proxy
LISTEN_HOST=127.0.0.1 LISTEN_PORT=18080 ./llm-proxy
```

The process reads `config.json` from its working directory by default. Override paths with `CONFIG_FILE`, `USAGE_FILE`, `METRICS_FILE`, `METRICS_COUNT_FILE`, and `RESPONSE_STATE_FILE`. Optional upstream proxy: `SOCKS5_PROXY` (also accepts Go-supported HTTP proxy URLs). The default listen address is `127.0.0.1:18080`; configure with `LISTEN_HOST` and `LISTEN_PORT`.

## Implemented endpoints

- `GET /health`
- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /v1/responses` (basic pass-through or basic Responses-to-Chat request conversion)
- `GET/PUT /api/config`
- `GET /api/usage`
- `GET /api/metrics`
- `GET /api/response-sessions` (currently placeholder)
- `GET /api/error-stats/{summary,models,timeline}`
- `POST /api/test` (sends a `你好` smoke-test request to the selected configured upstream)

## Important limitations

This is an early port, not yet feature-equivalent to the original Node.js application. In particular, full SSE streaming/event translation, complete Responses API semantics and conversation/session persistence, benchmark/test-all endpoints, and complete metrics/error timeline compatibility are not implemented. The existing Vue dashboard is not bundled; static serving expects a `dist/` directory in the working directory. Review and test before production use.

Configuration files and credentials are intentionally not included. Supply your own `config.json`; do not commit provider API keys.
