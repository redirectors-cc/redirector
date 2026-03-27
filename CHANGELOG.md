# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-03-27

### Added

- **Instant redirect mode** — when `interstitial.delay_seconds` is set to `0`, the server returns an HTTP 307 redirect directly to the target URL, bypassing the interstitial page entirely while still collecting analytics

## [0.1.0] - 2026-02-06

### Added

- **Hashid-based URL shortening** - Core redirect functionality
  - `/r/{hashid}` - Direct redirect
  - `/d/{hashid}` - Interstitial page with countdown timer
  - Multiple hashid salt support for migration scenarios

- **Caching & Storage**
  - Redis caching with configurable TTL
  - PostgreSQL storage backend
  - Circuit breaker for database protection
  - Rate limiting (global and database-level)

- **Event Analytics Pipeline**
  - RabbitMQ event publishing with fire-and-forget pattern (non-blocking)
  - Batching by size (100 events) and time interval (1 second)
  - Type-safe `EventBatch` enum with internally-tagged serde
  - Snowflake ID generation for batch identifiers (custom epoch 2025-01-01)
  - Separate `event_consumer` binary for running as own container
  - PostgreSQL analytics storage with monthly table partitioning
  - Auto-partition creation for `redirect_events` table
  - Reference tables with MD5 deduplication: `referers`, `user_agents`, `referer_domains`
  - User-Agent parsing with woothee (browser, version, OS, device type)
  - GeoIP enrichment with MaxMind mmdb support and hot-reload
  - Domain normalization (strips www., lowercases, removes trailing dots)
  - Graceful degradation: redirects work without RabbitMQ connection

- **Admin Dashboard**
  - Real-time monitoring with SSE-based live updates
  - RPS and latency charts using Chart.js
  - System metrics (CPU, memory, uptime)
  - Cache hit rate monitoring
  - Recent redirects list
  - Load simulation for testing (configurable RPS)
  - Session-based authentication with Argon2 password hashing

- **Environment Variable Configuration**
  - Full configuration via env vars without requiring config files
  - Required vars: `DATABASE_URL`, `REDIS_URL`, `HASHIDS_SALTS`, `METRICS_USERNAME`, `METRICS_PASSWORD`
  - JSON array support for `ADMIN_USERS`: `[{"username":"x","password_hash":"y"}]`
  - PaaS-friendly: works with Railway, Heroku, Render without config files
  - Loading priority: `CONFIG_BASE64` > `CONFIG_PATH`/`config.yaml` > env vars only

- **Observability**
  - Prometheus metrics with Basic Auth
  - Health check endpoints (`/healthz` for liveness, `/health` for readiness)
  - X-Version header for all responses

- **UI/UX**
  - Four color themes: Light, Dark, Gray, and Warm
  - Beautiful 404 "Link Not Found" page
  - Responsive design

- **Binaries**
  - `redirector` - Main service
  - `event_consumer` - Standalone consumer binary for analytics pipeline
  - `load_test` - HTTP load testing tool
  - `hash_password` - Password hashing utility
  - `gen_hashids` - Hashid generation for testing
  - `gen_simulation_data` - Simulation data generator

- **Infrastructure**
  - Docker and Docker Compose support
  - CI/CD with coverage and performance badges
  - 18 language translations for documentation

[0.1.2]: https://github.com/redirectors-cc/redirector/releases/tag/v0.1.2
[0.1.0]: https://github.com/redirectors-cc/redirector/releases/tag/v0.1.0
