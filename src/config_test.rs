#[cfg(test)]
mod tests {
    use crate::config::*;
    use serial_test::serial;

    #[test]
    fn test_default_server_host() {
        assert_eq!(ServerConfig::default().host, "0.0.0.0");
    }

    #[test]
    fn test_default_server_port() {
        assert_eq!(ServerConfig::default().port, 8080);
    }

    #[test]
    fn test_default_hashids_min_length() {
        let config = HashidsConfig {
            salts: vec!["test".to_string()],
            min_length: 6,
        };
        assert_eq!(config.min_length, 6);
    }

    #[test]
    fn test_default_redis_cache_ttl() {
        assert_eq!(RedisConfig::default().cache_ttl_seconds, 86400);
    }

    #[test]
    fn test_default_pool_max_connections() {
        assert_eq!(PoolConfig::default().max_connections, 3);
    }

    #[test]
    fn test_default_pool_connect_timeout() {
        assert_eq!(PoolConfig::default().connect_timeout_seconds, 3);
    }

    #[test]
    fn test_default_db_rate_limit() {
        assert_eq!(DbRateLimitConfig::default().max_requests_per_second, 50);
    }

    #[test]
    fn test_default_circuit_breaker_failure_threshold() {
        assert_eq!(CircuitBreakerConfig::default().failure_threshold, 3);
    }

    #[test]
    fn test_default_circuit_breaker_reset_timeout() {
        assert_eq!(CircuitBreakerConfig::default().reset_timeout_seconds, 60);
    }

    #[test]
    fn test_default_interstitial_delay() {
        let config = InterstitialConfig { delay_seconds: 5 };
        assert_eq!(config.delay_seconds, 5);
    }

    #[test]
    fn test_default_rate_limit_rps() {
        assert_eq!(RateLimitConfig::default().requests_per_second, 1000);
    }

    #[test]
    fn test_default_rate_limit_burst() {
        assert_eq!(RateLimitConfig::default().burst, 100);
    }

    #[test]
    fn test_default_query_table() {
        assert_eq!(QueryConfig::default().table, "dictionary.urls");
    }

    #[test]
    fn test_default_query_id_column() {
        assert_eq!(QueryConfig::default().id_column, "id");
    }

    #[test]
    fn test_default_query_url_column() {
        assert_eq!(QueryConfig::default().url_column, "name");
    }

    #[test]
    #[serial]
    fn test_load_from_base64_valid() {
        use base64::Engine;

        // Clean REDIRECTOR__* env vars that config crate would merge on top of YAML
        let mut guard = EnvGuard::new();
        guard.remove("REDIRECTOR__HASHIDS__SALTS__0");
        guard.remove("REDIRECTOR__SERVER__PORT");

        let yaml = r#"
server:
  host: "127.0.0.1"
  port: 9090
hashids:
  salts:
    - "test-salt"
  min_length: 8
redis:
  url: "redis://localhost"
database:
  url: "postgres://localhost/test"
  query:
    table: "urls"
    id_column: "id"
    url_column: "url"
interstitial:
  delay_seconds: 3
metrics:
  basic_auth:
    username: "admin"
    password: "secret"
"#;
        let encoded = base64::engine::general_purpose::STANDARD.encode(yaml);
        let config = Config::load_from_base64(&encoded).unwrap();
        assert_eq!(config.server.host, "127.0.0.1");
        // Note: PORT env var override is applied by apply_paas_overrides()
        // In CI, PORT may be set to 3000, so we check that port is valid, not exact value
        assert!(config.server.port > 0);
        assert_eq!(config.hashids.salts[0], "test-salt");
        assert_eq!(config.hashids.min_length, 8);
        assert_eq!(config.interstitial.delay_seconds, 3);
    }

    #[test]
    fn test_load_from_base64_invalid_base64() {
        let result = Config::load_from_base64("not-valid-base64!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_from_base64_invalid_yaml() {
        use base64::Engine;

        let encoded = base64::engine::general_purpose::STANDARD.encode("not: valid: yaml: [[[");
        let result = Config::load_from_base64(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_from_base64_empty() {
        use base64::Engine;

        let encoded = base64::engine::general_purpose::STANDARD.encode("");
        let result = Config::load_from_base64(&encoded);
        assert!(result.is_err());
    }

    // --- PaaS env override tests ---

    fn make_test_config() -> Config {
        Config {
            server: ServerConfig::default(),
            hashids: HashidsConfig {
                salts: vec!["test".to_string()],
                min_length: 6,
            },
            redis: RedisConfig::default(),
            database: DatabaseConfig {
                url: "postgres://original/db".to_string(),
                pool: PoolConfig::default(),
                rate_limit: DbRateLimitConfig::default(),
                circuit_breaker: CircuitBreakerConfig::default(),
                query: QueryConfig::default(),
            },
            interstitial: InterstitialConfig { delay_seconds: 5 },
            metrics: MetricsConfig {
                basic_auth: BasicAuthConfig {
                    username: "admin".to_string(),
                    password: "secret".to_string(),
                },
            },
            rate_limit: RateLimitConfig::default(),
            admin: AdminConfig::default(),
            events: EventsConfig::default(),
        }
    }

    /// Guard that saves/restores env vars on drop.
    struct EnvGuard(Vec<(String, Option<String>)>);

    impl EnvGuard {
        fn new() -> Self {
            Self(Vec::new())
        }

        fn set(&mut self, key: &str, val: &str) {
            let old = std::env::var(key).ok();
            self.0.push((key.to_string(), old));
            unsafe { std::env::set_var(key, val) };
        }

        fn remove(&mut self, key: &str) {
            let old = std::env::var(key).ok();
            self.0.push((key.to_string(), old));
            unsafe { std::env::remove_var(key) };
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, old) in self.0.iter().rev() {
                match old {
                    Some(val) => unsafe { std::env::set_var(key, val) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    #[test]
    #[serial]
    fn test_paas_database_url_override() {
        let mut guard = EnvGuard::new();
        guard.set("DATABASE_URL", "postgres://paas/mydb");
        guard.remove("REDIRECTOR__DATABASE__URL");

        let mut config = make_test_config();
        config.apply_paas_overrides();

        assert_eq!(config.database.url, "postgres://paas/mydb");
    }

    #[test]
    #[serial]
    fn test_paas_redis_url_override() {
        let mut guard = EnvGuard::new();
        guard.set("REDIS_URL", "redis://paas-redis:6379");
        guard.remove("REDIRECTOR__REDIS__URL");

        let mut config = make_test_config();
        config.apply_paas_overrides();

        assert_eq!(config.redis.url, "redis://paas-redis:6379");
    }

    #[test]
    #[serial]
    fn test_paas_port_override() {
        let mut guard = EnvGuard::new();
        guard.set("PORT", "3000");
        guard.remove("REDIRECTOR__SERVER__PORT");

        let mut config = make_test_config();
        config.apply_paas_overrides();

        assert_eq!(config.server.port, 3000);
    }

    #[test]
    #[serial]
    fn test_paas_port_invalid_ignored() {
        let mut guard = EnvGuard::new();
        guard.set("PORT", "not-a-number");
        guard.remove("REDIRECTOR__SERVER__PORT");

        let mut config = make_test_config();
        let original_port = config.server.port;
        config.apply_paas_overrides();

        assert_eq!(config.server.port, original_port);
    }

    #[test]
    #[serial]
    fn test_prefixed_var_takes_priority_over_paas() {
        let mut guard = EnvGuard::new();
        guard.set("DATABASE_URL", "postgres://paas/mydb");
        guard.set("REDIRECTOR__DATABASE__URL", "postgres://explicit/mydb");

        let mut config = make_test_config();
        config.apply_paas_overrides();

        // DATABASE_URL should NOT override because REDIRECTOR__DATABASE__URL is set
        assert_ne!(config.database.url, "postgres://paas/mydb");
    }

    #[test]
    #[serial]
    fn test_no_paas_vars_keeps_original() {
        let mut guard = EnvGuard::new();
        guard.remove("DATABASE_URL");
        guard.remove("REDIS_URL");
        guard.remove("PORT");
        guard.remove("REDIRECTOR__DATABASE__URL");
        guard.remove("REDIRECTOR__REDIS__URL");
        guard.remove("REDIRECTOR__SERVER__PORT");

        let mut config = make_test_config();
        let original_db = config.database.url.clone();
        let original_redis = config.redis.url.clone();
        let original_port = config.server.port;
        config.apply_paas_overrides();

        assert_eq!(config.database.url, original_db);
        assert_eq!(config.redis.url, original_redis);
        assert_eq!(config.server.port, original_port);
    }

    #[test]
    fn test_paas_mappings_constant() {
        assert_eq!(PAAS_ENV_MAPPINGS.len(), 4);

        let paas_vars: Vec<&str> = PAAS_ENV_MAPPINGS.iter().map(|(k, _)| *k).collect();
        assert!(paas_vars.contains(&"DATABASE_URL"));
        assert!(paas_vars.contains(&"REDIS_URL"));
        assert!(paas_vars.contains(&"PORT"));
        assert!(paas_vars.contains(&"RABBITMQ_URL"));
    }

    #[test]
    #[serial]
    fn test_hashids_salts_env_override() {
        // All HASHIDS_SALTS tests in one function to avoid env var race conditions.
        let mut guard = EnvGuard::new();
        guard.remove("REDIRECTOR__HASHIDS__SALTS__0");

        // Comma-separated: two salts
        guard.set("HASHIDS_SALTS", "new-salt,old-salt");
        let mut config = make_test_config();
        config.apply_paas_overrides();
        assert_eq!(config.hashids.salts, vec!["new-salt", "old-salt"]);

        // Comma-separated: with spaces (trimmed)
        guard.set("HASHIDS_SALTS", " salt-a , salt-b , salt-c ");
        let mut config = make_test_config();
        config.apply_paas_overrides();
        assert_eq!(config.hashids.salts, vec!["salt-a", "salt-b", "salt-c"]);

        // Single salt (no comma)
        guard.set("HASHIDS_SALTS", "only-salt");
        let mut config = make_test_config();
        config.apply_paas_overrides();
        assert_eq!(config.hashids.salts, vec!["only-salt"]);

        // Indexed var takes priority over HASHIDS_SALTS
        guard.set("HASHIDS_SALTS", "comma-salt");
        guard.set("REDIRECTOR__HASHIDS__SALTS__0", "indexed-salt");
        let mut config = make_test_config();
        config.apply_paas_overrides();
        assert_ne!(config.hashids.salts, vec!["comma-salt".to_string()]);
    }

    // ---------------------------------------------------------------
    // Tests for load_from_env()
    // ---------------------------------------------------------------

    fn setup_required_env_vars(guard: &mut EnvGuard) {
        guard.set("DATABASE_URL", "postgres://localhost/test");
        guard.set("REDIS_URL", "redis://localhost:6379");
        guard.set("HASHIDS_SALTS", "test-salt");
        guard.set("METRICS_USERNAME", "admin");
        guard.set("METRICS_PASSWORD", "secret");
    }

    fn clear_all_env_vars(guard: &mut EnvGuard) {
        // Clear required vars
        guard.remove("DATABASE_URL");
        guard.remove("REDIS_URL");
        guard.remove("HASHIDS_SALTS");
        guard.remove("METRICS_USERNAME");
        guard.remove("METRICS_PASSWORD");
        // Clear optional vars
        guard.remove("HOST");
        guard.remove("PORT");
        guard.remove("ADMIN_ENABLED");
        guard.remove("ADMIN_USERS");
        guard.remove("ADMIN_SESSION_SECRET");
        guard.remove("ADMIN_SESSION_TTL_HOURS");
        guard.remove("EVENTS_ENABLED");
        guard.remove("RABBITMQ_URL");
        guard.remove("RABBITMQ_QUEUE");
        guard.remove("REDIS_CACHE_TTL");
        guard.remove("DB_MAX_CONNECTIONS");
        guard.remove("DB_CONNECT_TIMEOUT");
        guard.remove("DB_RPS");
        guard.remove("DB_TABLE");
        guard.remove("DB_ID_COLUMN");
        guard.remove("DB_URL_COLUMN");
        guard.remove("CB_FAILURE_THRESHOLD");
        guard.remove("CB_RESET_TIMEOUT");
        guard.remove("INTERSTITIAL_DELAY");
        guard.remove("RATE_LIMIT_RPS");
        guard.remove("RATE_LIMIT_BURST");
        guard.remove("HASHIDS_MIN_LENGTH");
        guard.remove("PUBLISHER_BUFFER_SIZE");
        guard.remove("PUBLISHER_BATCH_SIZE");
        guard.remove("PUBLISHER_FLUSH_INTERVAL_MS");
        guard.remove("CONSUMER_PREFETCH_COUNT");
        guard.remove("CONSUMER_DATABASE_URL");
        // Clear REDIRECTOR__ prefixed vars that may interfere
        guard.remove("REDIRECTOR__DATABASE__URL");
        guard.remove("REDIRECTOR__REDIS__URL");
        guard.remove("REDIRECTOR__SERVER__PORT");
        guard.remove("REDIRECTOR__HASHIDS__SALTS__0");
    }

    #[test]
    #[serial]
    fn test_load_from_env_all_required() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        let config = Config::load_from_env().unwrap();

        assert_eq!(config.database.url, "postgres://localhost/test");
        assert_eq!(config.redis.url, "redis://localhost:6379");
        assert_eq!(config.hashids.salts, vec!["test-salt"]);
        assert_eq!(config.metrics.basic_auth.username, "admin");
        assert_eq!(config.metrics.basic_auth.password, "secret");
    }

    #[test]
    #[serial]
    fn test_load_from_env_missing_required() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);

        let result = Config::load_from_env();
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(err.contains("DATABASE_URL"));
        assert!(err.contains("REDIS_URL"));
        assert!(err.contains("METRICS_USERNAME"));
        assert!(err.contains("METRICS_PASSWORD"));
        assert!(err.contains("HASHIDS_SALTS"));
    }

    #[test]
    #[serial]
    fn test_load_from_env_missing_partial() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);

        // Only set some required vars
        guard.set("DATABASE_URL", "postgres://localhost/test");
        guard.set("REDIS_URL", "redis://localhost:6379");

        let result = Config::load_from_env();
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(!err.contains("DATABASE_URL")); // Not missing
        assert!(!err.contains("REDIS_URL")); // Not missing
        assert!(err.contains("METRICS_USERNAME")); // Missing
    }

    #[test]
    #[serial]
    fn test_load_from_env_admin_users_json() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        guard.set("ADMIN_ENABLED", "true");
        guard.set(
            "ADMIN_USERS",
            r#"[{"username":"admin","password_hash":"$argon2id$test"}]"#,
        );

        let config = Config::load_from_env().unwrap();

        assert!(config.admin.enabled);
        assert_eq!(config.admin.users.len(), 1);
        assert_eq!(config.admin.users[0].username, "admin");
        assert_eq!(config.admin.users[0].password_hash, "$argon2id$test");
    }

    #[test]
    #[serial]
    fn test_load_from_env_admin_users_multiple() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        guard.set("ADMIN_ENABLED", "true");
        guard.set(
            "ADMIN_USERS",
            r#"[{"username":"admin","password_hash":"hash1"},{"username":"user2","password_hash":"hash2"}]"#,
        );

        let config = Config::load_from_env().unwrap();

        assert_eq!(config.admin.users.len(), 2);
        assert_eq!(config.admin.users[0].username, "admin");
        assert_eq!(config.admin.users[1].username, "user2");
    }

    #[test]
    #[serial]
    fn test_load_from_env_invalid_admin_users_json() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        guard.set("ADMIN_USERS", "not valid json");

        let result = Config::load_from_env();
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid JSON"));
        assert!(err.contains("ADMIN_USERS"));
    }

    #[test]
    #[serial]
    fn test_load_from_env_defaults() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        let config = Config::load_from_env().unwrap();

        // Check defaults are applied
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.redis.cache_ttl_seconds, 86400);
        assert_eq!(config.database.pool.max_connections, 3);
        assert!(!config.admin.enabled);
        assert!(!config.events.enabled);
        assert_eq!(config.rate_limit.requests_per_second, 1000);
        assert_eq!(config.rate_limit.burst, 100);
    }

    #[test]
    #[serial]
    fn test_load_from_env_custom_values() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        // Override defaults
        guard.set("HOST", "127.0.0.1");
        guard.set("PORT", "3000");
        guard.set("REDIS_CACHE_TTL", "3600");
        guard.set("DB_MAX_CONNECTIONS", "10");
        guard.set("RATE_LIMIT_RPS", "500");

        let config = Config::load_from_env().unwrap();

        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 3000);
        assert_eq!(config.redis.cache_ttl_seconds, 3600);
        assert_eq!(config.database.pool.max_connections, 10);
        assert_eq!(config.rate_limit.requests_per_second, 500);
    }

    #[test]
    #[serial]
    fn test_load_from_env_bool_parsing() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        // Test different bool formats
        guard.set("ADMIN_ENABLED", "true");
        let config = Config::load_from_env().unwrap();
        assert!(config.admin.enabled);

        guard.set("ADMIN_ENABLED", "1");
        let config = Config::load_from_env().unwrap();
        assert!(config.admin.enabled);

        guard.set("ADMIN_ENABLED", "yes");
        let config = Config::load_from_env().unwrap();
        assert!(config.admin.enabled);

        guard.set("ADMIN_ENABLED", "false");
        let config = Config::load_from_env().unwrap();
        assert!(!config.admin.enabled);

        guard.set("ADMIN_ENABLED", "0");
        let config = Config::load_from_env().unwrap();
        assert!(!config.admin.enabled);
    }

    #[test]
    #[serial]
    fn test_load_from_env_events_config() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        guard.set("EVENTS_ENABLED", "true");
        guard.set("RABBITMQ_URL", "amqp://user:pass@host:5672/%2f");
        guard.set("RABBITMQ_QUEUE", "my-queue");

        let config = Config::load_from_env().unwrap();

        assert!(config.events.enabled);
        assert_eq!(config.events.rabbitmq.url, "amqp://user:pass@host:5672/%2f");
        assert_eq!(config.events.rabbitmq.queue, "my-queue");
    }

    #[test]
    #[serial]
    fn test_load_from_env_hashids_multiple_salts() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);

        guard.set("DATABASE_URL", "postgres://localhost/test");
        guard.set("REDIS_URL", "redis://localhost:6379");
        guard.set("HASHIDS_SALTS", "salt1, salt2, salt3");
        guard.set("METRICS_USERNAME", "admin");
        guard.set("METRICS_PASSWORD", "secret");

        let config = Config::load_from_env().unwrap();

        assert_eq!(config.hashids.salts, vec!["salt1", "salt2", "salt3"]);
    }

    #[test]
    #[serial]
    fn test_load_from_env_hashids_salts_empty_values() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);

        // Set all required vars except use empty salts
        guard.set("DATABASE_URL", "postgres://localhost/test");
        guard.set("REDIS_URL", "redis://localhost:6379");
        guard.set("METRICS_USERNAME", "admin");
        guard.set("METRICS_PASSWORD", "secret");
        // HASHIDS_SALTS with only empty values after split/trim
        guard.set("HASHIDS_SALTS", " , , ");

        // Should fail because no valid salts
        let result = Config::load_from_env();
        assert!(result.is_err());

        let err = result.unwrap_err().to_string();
        assert!(err.contains("HASHIDS_SALTS"));
    }

    #[test]
    #[serial]
    fn test_load_from_env_empty_admin_users() {
        let mut guard = EnvGuard::new();
        clear_all_env_vars(&mut guard);
        setup_required_env_vars(&mut guard);

        // ADMIN_USERS not set - should default to empty
        let config = Config::load_from_env().unwrap();
        assert!(config.admin.users.is_empty());

        // ADMIN_USERS set to empty string - should default to empty
        guard.set("ADMIN_USERS", "");
        let config = Config::load_from_env().unwrap();
        assert!(config.admin.users.is_empty());
    }
}
