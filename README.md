# LLM Gateway

Легковесный OpenAI-совместимый HTTP-прокси на Go на базе [Bifrost Core](https://github.com/maximhq/bifrost).

Шлюз инкапсулирует работу с upstream-провайдерами: клиенты обращаются к виртуальным логическим моделям (например, `standard`, `fast`), не зная реальных идентификаторов моделей, провайдеров, внутренних URL и API-ключей.

## Возможности

- **OpenAI-совместимость:** поддержка `/v1/chat/completions` и `/v1/responses` в обычном и streaming (SSE) режимах.
- **Логические модели:** сокрытие топологии и учетных данных провайдеров от клиентов.
- **Декларативный роутинг:** цепочки правил (`filter`, `map`, `admission`, `balance`, `affinity`, `race`, `retry`, `hedge`, `timeout`).
- **Балансировка и отказоустойчивость:** балансировка по ожидаемому TTFT (EWMA), автоматический cooldown при сбоях (429, 5xx), многоуровневый fallback (tiers) и hedging запросов.
- **Управление стримами:** детекция обрывов (mid-stream breaks), защита от зацикливания ответов (loop guard) и idle-таймауты.
- **Наблюдаемость:** Prometheus-метрики на отдельном loopback-порту и структурированные JSON-события (`log/slog`) с трассировкой по `request_id`.
- **Безопасность:** авторизация клиентов по Bearer-токенам, безопасная передача секретов через переменные окружения (`env.VARIABLE_NAME`).

---

## API Endpoints

| Метод | Путь | Назначение | Авторизация |
| --- | --- | --- | --- |
| `POST` | `/v1/chat/completions` | OpenAI Chat Completions (streaming / non-streaming) | Bearer токен |
| `POST` | `/v1/responses` | OpenAI Responses API (streaming / non-streaming) | Bearer токен |
| `GET` | `/v1/models` | Список доступных логических моделей | Bearer токен |
| `GET` | `/healthz` | Health check (`{"status":"ok"}`) | Без авторизации |
| `POST` | `/admin/models/refresh` | Принудительное обновление каталогов моделей | Bearer токен |
| `GET` | `/metrics` | Prometheus-метрики (по умолчанию `127.0.0.1:9209`) | Loopback, без токена |

Если в конфигурации задан список `client_api_keys`, эндпоинты `/v1/*` и `/admin/*` требуют заголовок:

```text
Authorization: Bearer <client_api_key>
```

---

## Быстрый старт

### Сборка

```sh
# Сборка бинарного файла в bin/llm-gateway
make build

# Или напрямую через Go:
go build -o bin/llm-gateway ./cmd/llm-gateway
```

### Проверка конфигурации

```sh
./bin/llm-gateway --config configs/config.example.json check
# или через make:
make check-config
```

### Запуск сервиса

```sh
./bin/llm-gateway --config configs/config.example.json serve
```

---

## Конфигурация

Конфигурация задается в формате JSON (полный пример см. в [configs/config.example.json](configs/config.example.json)). Значения секретов поддерживают загрузку из переменных окружения с префиксом `env.` (например, `"api_key": "env.OPENAI_API_KEY"`).

### Пример конфигурации

```json
{
  "host": "127.0.0.1",
  "port": 9208,
  "metrics_host": "127.0.0.1",
  "metrics_port": 9209,
  "log_level": "info",
  "catalog_refresh_interval": "10m",
  "client_api_keys": [
    {
      "id": "app-client",
      "key": "env.CLIENT_API_KEY"
    }
  ],
  "providers": [
    {
      "id": "upstream-primary",
      "base_provider": "openai",
      "inference_url": "https://api.openai.com/v1",
      "api_key": "env.OPENAI_API_KEY",
      "priority": 10,
      "cooldown": "15s",
      "request_timeout": "60s"
    },
    {
      "id": "upstream-backup",
      "base_provider": "openai",
      "inference_url": "https://api.backup-provider.com/v1",
      "api_key": "env.BACKUP_API_KEY",
      "priority": 5,
      "cooldown": "15s",
      "request_timeout": "60s"
    }
  ],
  "routing_rules": [
    { "route": "standard", "action": "filter", "where": { "model": { "eq": "standard" } } },
    { "route": "standard", "action": "filter", "where": { "provider": { "in": ["upstream-primary", "upstream-backup"] } } },
    { "route": "standard", "action": "map", "native": "gpt-4o" },
    { "route": "standard", "action": "rank", "strategy": "priority" },
    { "route": "standard", "action": "race", "count": 1 },
    {
      "route": "standard",
      "action": "retry",
      "target": "standard.retry",
      "attempts": 2,
      "backoff": { "type": "exponential", "initial": "200ms", "max": "1s" }
    },
    { "route": "standard", "action": "timeout", "duration": "60s" },

    { "route": "standard.retry", "action": "filter", "where": { "error": { "in": ["429", "5xx", "timeout", "connection_error"] } } },
    { "route": "standard.retry", "action": "filter", "where": { "provider": { "in": ["upstream-primary", "upstream-backup"], "unused": true } } },
    { "route": "standard.retry", "action": "map", "native": "gpt-4o" },
    { "route": "standard.retry", "action": "rank", "strategy": "priority" },
    { "route": "standard.retry", "action": "race", "count": 1 }
  ]
}
```

---

## Маршрутизация (Routing Rules)

Маршрутизация задается плоским списком правил (`routing_rules`). Каждое правило относится к определенному маршруту (`route`) и выполняет конкретное действие (`action`):

```text
filter → map → admission → balance → affinity → race → retry → hedge → timeout
```

- **`filter`**: фильтрация запросов по имени модели (`where.model.eq`), списку провайдеров (`where.provider.in`), классу ошибки (`where.error.in`) или статусу использования (`unused: true`).
- **`map`**: сопоставление виртуальной модели с реальной моделью провайдера (`native`), с опциональной привязкой к уровню (`tier`).
- **`admission`**: контроль очередей и конкурентности (`maxInFlight`, `maxPending`, `waitTimeout`).
- **`balance`**: динамический выбор таргета (например, по ожидаемой задержке первого токена: `expected-ttft` с поддержкой $\epsilon$-greedy exploration).
- **`affinity`**: привязка цепочек Responses API к выигравшему провайдеру по сессионному идентификатору.
- **`race`**: количество параллельных запросов (`count`) к лучшим провайдерам (первый успешный ответ выигрывает).
- **`retry`**: маршрутизация при ошибках (429, 5xx, таймауты) на резервные провайдеры с настройкой экспоненциального бэкоффа.
- **`hedge`**: запуск параллельного резервного запроса, если основной не ответил за указанное время (`after`).
- **`timeout`**: ограничение общего времени запроса (`duration`) и допустимой паузы между чанками стрима (`idle`).

---

## Наблюдаемость

### Метрики Prometheus

Метрики отдаются в формате Prometheus на выделенном loopback-адресе (`127.0.0.1:9209/metrics`).

Проверка сбора метрик:

```sh
curl -s localhost:9209/metrics
```

Ключевые метрики:
- `llm_requests_total` — счетчик завершенных запросов по статусам, маршрутам и провайдерам.
- `llm_request_duration_seconds` — гистограмма полного времени выполнения запросов.
- `llm_ttft_seconds` — время до получения первого токена (Time to First Token).
- `llm_input_tokens_total` / `llm_output_tokens_total` — учет использованных токенов.
- `llm_attempts_total` / `llm_stream_breaks_total` — upstream-попытки и обрывы стримов.
- `llm_requests_in_flight` — текущее количество активных запросов по провайдерам.
- `llm_cooldown_until_seconds` — время окончания кулдауна провайдера/модели при сбоях.

### Структурированные логи

Логирование осуществляется через стандартный Go-пакет `log/slog` в формате JSON (в `stdout`). Каждый запрос маркируется уникальным `request_id`, связывающим события попыток (`request_received`, `llm_attempt`, `llm_retry`, `request_completed`, `request_failed`). Тела запросов, промпты и ключи авторизации не логируются.

---

## Тестирование и линтинг

```sh
make test       # запуск модульных тестов
make test-race  # запуск тестов с проверкой гонок (race detector)
make lint       # линтинг (golangci-lint или go vet)
```
