use axum::{body::Body, http::Request, routing::get, Router};
use redirector::config::{DatabaseConfig, HashidsConfig, RedisConfig};
use redirector::db::MainStorage;
use redirector::handlers::{redirect_handler, RedirectState};
use redirector::services::{CacheService, HashidService, UrlResolver};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tower::ServiceExt;

async fn setup_services(
    redis_port: u16,
    postgres_port: u16,
) -> Arc<RedirectState<HashidService, CacheService, MainStorage>> {
    setup_services_with_delay(redis_port, postgres_port, 5).await
}

async fn setup_services_with_delay(
    redis_port: u16,
    postgres_port: u16,
    delay_seconds: u32,
) -> Arc<RedirectState<HashidService, CacheService, MainStorage>> {
    let hashid_config = HashidsConfig {
        salts: vec!["test_salt".to_string()],
        min_length: 6,
    };

    let redis_config = RedisConfig {
        url: format!("redis://localhost:{}", redis_port),
        cache_ttl_seconds: 60,
    };

    let database_config = DatabaseConfig {
        url: format!(
            "postgres://redirector:password@localhost:{}/redirector",
            postgres_port
        ),
        pool: Default::default(),
        rate_limit: Default::default(),
        circuit_breaker: Default::default(),
        query: Default::default(),
    };

    let hashid_service = Arc::new(HashidService::new(&hashid_config));
    let cache_service = Arc::new(CacheService::new(&redis_config).expect("Failed to create cache"));
    let main_storage = Arc::new(
        MainStorage::new(&database_config)
            .await
            .expect("Failed to create storage"),
    );

    let url_resolver = Arc::new(UrlResolver::new(
        hashid_service,
        cache_service,
        main_storage,
    ));

    Arc::new(RedirectState {
        resolver: url_resolver,
        delay_seconds,
        dispatcher: redirector::events::dispatcher::EventDispatcher::noop(),
    })
}

fn create_app(state: Arc<RedirectState<HashidService, CacheService, MainStorage>>) -> Router {
    Router::new()
        .route("/r/{hashid}", get(redirect_handler))
        .with_state(state)
}

async fn setup_database(pool: &sqlx::PgPool) {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS dictionary")
        .execute(pool)
        .await
        .expect("Failed to create schema");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dictionary.urls (
            id BIGINT PRIMARY KEY,
            name VARCHAR(4096) NOT NULL
        )",
    )
    .execute(pool)
    .await
    .expect("Failed to create table");
}

#[tokio::test]
async fn test_redirect_valid_hashid_returns_interstitial() {
    // Start Redis
    let redis = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis");
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Start Postgres with test data
    let postgres = Postgres::default()
        .with_db_name("redirector")
        .with_user("redirector")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres");
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    // Create schema and insert test data
    let db_url = format!(
        "postgres://redirector:password@localhost:{}/redirector",
        postgres_port
    );
    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    setup_database(&pool).await;

    sqlx::query("INSERT INTO dictionary.urls (id, name) VALUES (1, 'https://example.com')")
        .execute(&pool)
        .await
        .expect("Failed to insert data");

    sqlx::query("INSERT INTO dictionary.urls (id, name) VALUES (2, 'https://github.com')")
        .execute(&pool)
        .await
        .expect("Failed to insert data");

    // Setup services
    let state = setup_services(redis_port, postgres_port).await;
    let app = create_app(state);

    // Encode ID 1 with salt "test_salt" to get hashid
    let hashids = harsh::Harsh::builder()
        .salt("test_salt")
        .length(6)
        .build()
        .unwrap();
    let hashid = hashids.encode(&[1]);

    // Make request
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/r/{}", hashid))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make request");

    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Check that interstitial page contains target URL
    assert!(body_str.contains("example.com"));
    assert!(body_str.contains("https://example.com"));
}

#[tokio::test]
async fn test_redirect_invalid_hashid_returns_404() {
    // Start Redis
    let redis = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis");
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Start Postgres
    let postgres = Postgres::default()
        .with_db_name("redirector")
        .with_user("redirector")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres");
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    // Create schema
    let db_url = format!(
        "postgres://redirector:password@localhost:{}/redirector",
        postgres_port
    );
    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    setup_database(&pool).await;

    // Setup services
    let state = setup_services(redis_port, postgres_port).await;
    let app = create_app(state);

    // Make request with invalid hashid
    let response = app
        .oneshot(
            Request::builder()
                .uri("/r/invalid_hashid_12345")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make request");

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_redirect_nonexistent_id_returns_404() {
    // Start Redis
    let redis = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis");
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Start Postgres
    let postgres = Postgres::default()
        .with_db_name("redirector")
        .with_user("redirector")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres");
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    // Create schema (empty table)
    let db_url = format!(
        "postgres://redirector:password@localhost:{}/redirector",
        postgres_port
    );
    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    setup_database(&pool).await;

    // Setup services
    let state = setup_services(redis_port, postgres_port).await;
    let app = create_app(state);

    // Encode ID 99999 (doesn't exist in DB)
    let hashids = harsh::Harsh::builder()
        .salt("test_salt")
        .length(6)
        .build()
        .unwrap();
    let hashid = hashids.encode(&[99999]);

    // Make request
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/r/{}", hashid))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make request");

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_redirect_uses_cache_on_second_request() {
    // Start Redis
    let redis = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis");
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Start Postgres
    let postgres = Postgres::default()
        .with_db_name("redirector")
        .with_user("redirector")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres");
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    // Create schema and insert test data
    let db_url = format!(
        "postgres://redirector:password@localhost:{}/redirector",
        postgres_port
    );
    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    setup_database(&pool).await;

    sqlx::query("INSERT INTO dictionary.urls (id, name) VALUES (1, 'https://cached.example.com')")
        .execute(&pool)
        .await
        .expect("Failed to insert data");

    // Setup services
    let state = setup_services(redis_port, postgres_port).await;

    // Encode ID 1
    let hashids = harsh::Harsh::builder()
        .salt("test_salt")
        .length(6)
        .build()
        .unwrap();
    let hashid = hashids.encode(&[1]);

    // First request - should hit DB
    let app1 = create_app(state.clone());
    let response1 = app1
        .oneshot(
            Request::builder()
                .uri(format!("/r/{}", hashid))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make first request");

    assert_eq!(response1.status(), 200);

    // Delete from DB to prove cache is used
    sqlx::query("DELETE FROM dictionary.urls WHERE id = 1")
        .execute(&pool)
        .await
        .expect("Failed to delete");

    // Second request - should hit cache (not DB)
    let app2 = create_app(state);
    let response2 = app2
        .oneshot(
            Request::builder()
                .uri(format!("/r/{}", hashid))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make second request");

    assert_eq!(response2.status(), 200);

    let body = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Should still contain the URL from cache
    assert!(body_str.contains("cached.example.com"));
}

#[tokio::test]
async fn test_redirect_interstitial_contains_countdown() {
    // Start Redis
    let redis = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis");
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Start Postgres
    let postgres = Postgres::default()
        .with_db_name("redirector")
        .with_user("redirector")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres");
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    // Create schema and insert test data
    let db_url = format!(
        "postgres://redirector:password@localhost:{}/redirector",
        postgres_port
    );
    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    setup_database(&pool).await;

    sqlx::query("INSERT INTO dictionary.urls (id, name) VALUES (1, 'https://countdown.test')")
        .execute(&pool)
        .await
        .expect("Failed to insert data");

    // Setup services
    let state = setup_services(redis_port, postgres_port).await;
    let app = create_app(state);

    // Encode ID 1
    let hashids = harsh::Harsh::builder()
        .salt("test_salt")
        .length(6)
        .build()
        .unwrap();
    let hashid = hashids.encode(&[1]);

    // Make request
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/r/{}", hashid))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make request");

    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Check interstitial page elements
    assert!(body_str.contains("countdown") || body_str.contains("Countdown"));
    assert!(body_str.contains("5")); // delay_seconds
}

#[tokio::test]
async fn test_redirect_zero_delay_returns_302() {
    // Start Redis
    let redis = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis");
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Start Postgres with test data
    let postgres = Postgres::default()
        .with_db_name("redirector")
        .with_user("redirector")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres");
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    // Create schema and insert test data
    let db_url = format!(
        "postgres://redirector:password@localhost:{}/redirector",
        postgres_port
    );
    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    setup_database(&pool).await;

    sqlx::query(
        "INSERT INTO dictionary.urls (id, name) VALUES (1, 'https://instant.example.com/target')",
    )
    .execute(&pool)
    .await
    .expect("Failed to insert data");

    // Setup services with delay_seconds = 0
    let state = setup_services_with_delay(redis_port, postgres_port, 0).await;
    let app = create_app(state);

    // Encode ID 1
    let hashids = harsh::Harsh::builder()
        .salt("test_salt")
        .length(6)
        .build()
        .unwrap();
    let hashid = hashids.encode(&[1]);

    // Make request
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/r/{}", hashid))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make request");

    // Should return 307 Temporary Redirect, not 200 with interstitial
    assert_eq!(response.status(), 307);

    // Location header should point to target URL
    let location = response
        .headers()
        .get("location")
        .expect("Missing Location header")
        .to_str()
        .unwrap();
    assert_eq!(location, "https://instant.example.com/target");
}

#[tokio::test]
async fn test_redirect_with_headers() {
    // Start Redis
    let redis = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis");
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Start Postgres
    let postgres = Postgres::default()
        .with_db_name("redirector")
        .with_user("redirector")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres");
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    // Create schema and insert test data
    let db_url = format!(
        "postgres://redirector:password@localhost:{}/redirector",
        postgres_port
    );
    let pool = sqlx::PgPool::connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    setup_database(&pool).await;

    sqlx::query("INSERT INTO dictionary.urls (id, name) VALUES (1, 'https://headers.test')")
        .execute(&pool)
        .await
        .expect("Failed to insert data");

    // Setup services
    let state = setup_services(redis_port, postgres_port).await;
    let app = create_app(state);

    // Encode ID 1
    let hashids = harsh::Harsh::builder()
        .salt("test_salt")
        .length(6)
        .build()
        .unwrap();
    let hashid = hashids.encode(&[1]);

    // Make request WITH headers
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/r/{}", hashid))
                .header("Referer", "https://google.com/search?q=test")
                .header("User-Agent", "Mozilla/5.0 Test Browser")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("Failed to make request");

    assert_eq!(response.status(), 200);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    assert!(body_str.contains("headers.test"));
}
