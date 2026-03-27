use async_trait::async_trait;
use chrono::Datelike;
use serde::Deserialize;
use sqlx::postgres::PgConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use super::snowflake::{create_generator, SnowflakeGenerator};
use super::traits::EventStorage;
use super::{DataSource, EventBatch};
use crate::config::EventsConfig;

/// Reserved ID in reference tables for unknown/missing values.
const UNKNOWN_REFERENCE_ID: i64 = 1;

/// How often to check the .mmdb file for updates.
const GEOIP_CHECK_INTERVAL: Duration = Duration::from_secs(3600);

// ── Key types (enriched data ready for DB persistence) ──────────────

/// Hash-based key for referers table.
struct HashKey {
    hash: String,
    value: String,
}

/// Key for user_agents table — hash + parsed UA fields.
struct UserAgentKey {
    hash: String,
    value: String,
    browser: Option<String>,
    browser_version: Option<String>,
    os: Option<String>,
    device_type: Option<String>,
}

/// Key for referer_domains table — normalized domain.
struct DomainKey {
    domain: String,
}

/// Key for geo_locations table — country + city pair.
struct GeoLocationKey {
    country_code: String,
    city: String,
}

// ── EntityResolver trait ────────────────────────────────────────────

/// Generic trait for reference table entities.
/// Three layers: enrichment (prepare), search (find), persistence (insert).
trait EntityResolver: Send + Sync {
    type Key: Send + Sync;

    /// Layer 1: Enrich raw input into a lookup key.
    /// Returns None for missing/invalid input → maps to UNKNOWN_REFERENCE_ID.
    fn prepare(&self, raw: &str) -> Option<Self::Key>;

    /// Layer 2: Search — SELECT id by key.
    async fn find(&self, conn: &mut PgConnection, key: &Self::Key) -> anyhow::Result<Option<i64>>;

    /// Layer 3: Persist — INSERT with application-generated Snowflake ID.
    /// ON CONFLICT DO NOTHING RETURNING id.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        id: i64,
        key: &Self::Key,
    ) -> anyhow::Result<Option<i64>>;
}

/// Resolve raw input to reference table ID via 3-step pattern:
/// find → insert (with Snowflake ID) → fallback find.
/// Monomorphized per resolver type — zero runtime overhead.
async fn resolve_or_create<R: EntityResolver>(
    resolver: &R,
    conn: &mut PgConnection,
    snowflake: &SnowflakeGenerator,
    raw: Option<&str>,
) -> anyhow::Result<i64> {
    let key = match raw.and_then(|v| resolver.prepare(v)) {
        Some(k) => k,
        None => return Ok(UNKNOWN_REFERENCE_ID),
    };

    if let Some(id) = resolver.find(conn, &key).await? {
        return Ok(id);
    }
    let new_id = snowflake.generate();
    if let Some(id) = resolver.insert(conn, new_id, &key).await? {
        return Ok(id);
    }
    // Race condition fallback
    resolver
        .find(conn, &key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Failed to resolve entity after insert"))
}

// ── Resolver: Referers ──────────────────────────────────────────────

/// Resolves referer strings to IDs via MD5 hash.
struct RefererResolver;

impl EntityResolver for RefererResolver {
    type Key = HashKey;

    fn prepare(&self, raw: &str) -> Option<Self::Key> {
        Some(HashKey {
            hash: format!("{:x}", md5::compute(raw.as_bytes())),
            value: raw.to_string(),
        })
    }

    async fn find(&self, conn: &mut PgConnection, key: &Self::Key) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM referers WHERE hash = $1 LIMIT 1")
            .bind(&key.hash)
            .fetch_optional(&mut *conn)
            .await?;
        Ok(row.map(|(id,)| id))
    }

    async fn insert(
        &self,
        conn: &mut PgConnection,
        id: i64,
        key: &Self::Key,
    ) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "INSERT INTO referers (id, hash, value) VALUES ($1, $2, $3) \
             ON CONFLICT (hash) DO NOTHING RETURNING id",
        )
        .bind(id)
        .bind(&key.hash)
        .bind(&key.value)
        .fetch_optional(&mut *conn)
        .await?;
        Ok(row.map(|(id,)| id))
    }
}

// ── Resolver: User Agents ───────────────────────────────────────────

/// Resolves User-Agent strings to IDs, parsing structured fields on first insert.
struct UserAgentResolver {
    parser: woothee::parser::Parser,
}

impl EntityResolver for UserAgentResolver {
    type Key = UserAgentKey;

    fn prepare(&self, raw: &str) -> Option<Self::Key> {
        let hash = format!("{:x}", md5::compute(raw.as_bytes()));
        let (browser, browser_version, os, device_type) = match self.parser.parse(raw) {
            Some(r) => (
                Some(r.name.to_string()),
                Some(r.version.to_string()),
                Some(r.os.to_string()),
                Some(r.category.to_string()),
            ),
            None => (None, None, None, None),
        };
        Some(UserAgentKey {
            hash,
            value: raw.to_string(),
            browser,
            browser_version,
            os,
            device_type,
        })
    }

    async fn find(&self, conn: &mut PgConnection, key: &Self::Key) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM user_agents WHERE hash = $1 LIMIT 1")
                .bind(&key.hash)
                .fetch_optional(&mut *conn)
                .await?;
        Ok(row.map(|(id,)| id))
    }

    async fn insert(
        &self,
        conn: &mut PgConnection,
        id: i64,
        key: &Self::Key,
    ) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "INSERT INTO user_agents (id, hash, value, browser, browser_version, os, device_type) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (hash) DO NOTHING RETURNING id",
        )
        .bind(id)
        .bind(&key.hash)
        .bind(&key.value)
        .bind(&key.browser)
        .bind(&key.browser_version)
        .bind(&key.os)
        .bind(&key.device_type)
        .fetch_optional(&mut *conn)
        .await?;
        Ok(row.map(|(id,)| id))
    }
}

// ── Resolver: Referer Domains ───────────────────────────────────────

/// Resolves referer URLs to normalized domain IDs.
struct RefererDomainResolver;

impl EntityResolver for RefererDomainResolver {
    type Key = DomainKey;

    fn prepare(&self, raw: &str) -> Option<Self::Key> {
        Some(DomainKey {
            domain: normalize_domain(raw),
        })
    }

    async fn find(&self, conn: &mut PgConnection, key: &Self::Key) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM referer_domains WHERE domain = $1 LIMIT 1")
                .bind(&key.domain)
                .fetch_optional(&mut *conn)
                .await?;
        Ok(row.map(|(id,)| id))
    }

    async fn insert(
        &self,
        conn: &mut PgConnection,
        id: i64,
        key: &Self::Key,
    ) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "INSERT INTO referer_domains (id, domain) VALUES ($1, $2) \
             ON CONFLICT (domain) DO NOTHING RETURNING id",
        )
        .bind(id)
        .bind(&key.domain)
        .fetch_optional(&mut *conn)
        .await?;
        Ok(row.map(|(id,)| id))
    }
}

// ── GeoIP state and deserialization ─────────────────────────────────

/// GeoIP reader state with hot-reload support.
struct GeoIpState {
    reader: Option<maxminddb::Reader<Vec<u8>>>,
    path: Option<String>,
    file_modified: Option<SystemTime>,
    last_check: Instant,
}

#[derive(Deserialize)]
struct GeoIpResult<'a> {
    #[serde(borrow)]
    country: Option<GeoIpCountry<'a>>,
    #[serde(borrow)]
    city: Option<GeoIpCity<'a>>,
}

#[derive(Deserialize)]
struct GeoIpCountry<'a> {
    iso_code: Option<&'a str>,
}

#[derive(Deserialize)]
struct GeoIpCity<'a> {
    #[serde(borrow)]
    names: Option<BTreeMap<&'a str, &'a str>>,
}

// ── Resolver: Geo Locations ─────────────────────────────────────────

/// Resolves IP addresses to geo location IDs via MaxMind GeoIP.
struct GeoLocationResolver {
    geoip: Mutex<GeoIpState>,
}

impl GeoLocationResolver {
    /// Check if the .mmdb file has been updated and reload if needed.
    fn maybe_reload_geoip(state: &mut GeoIpState) {
        let path = match &state.path {
            Some(p) => p.clone(),
            None => return,
        };

        if state.last_check.elapsed() < GEOIP_CHECK_INTERVAL {
            return;
        }

        state.last_check = Instant::now();

        let current_modified = match std::fs::metadata(&path).and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, path, "Failed to stat GeoIP database file");
                return;
            }
        };

        if state.file_modified == Some(current_modified) {
            return;
        }

        match maxminddb::Reader::open_readfile(&path) {
            Ok(reader) => {
                let old_modified = state.file_modified;
                state.reader = Some(reader);
                state.file_modified = Some(current_modified);
                tracing::info!(
                    path,
                    ?old_modified,
                    new_modified = ?current_modified,
                    "Reloaded GeoIP database (file changed on disk)"
                );
                metrics::counter!("geoip_reloads_total").increment(1);
            }
            Err(e) => {
                tracing::error!(error = %e, path, "Failed to reload GeoIP database");
            }
        }
    }
}

impl EntityResolver for GeoLocationResolver {
    type Key = GeoLocationKey;

    fn prepare(&self, raw: &str) -> Option<Self::Key> {
        let mut geoip = self.geoip.lock().unwrap();
        Self::maybe_reload_geoip(&mut geoip);

        let reader = geoip.reader.as_ref()?;
        let addr: IpAddr = raw.parse().ok()?;
        let lookup = reader.lookup(addr).ok()?;
        let result: Option<GeoIpResult<'_>> = lookup.decode().ok()?;
        let result = result?;

        let country_code = result
            .country
            .and_then(|c| c.iso_code)
            .unwrap_or("--")
            .to_string();
        let city = result
            .city
            .and_then(|c| c.names)
            .and_then(|n: BTreeMap<&str, &str>| n.get("en").copied())
            .unwrap_or("(unknown)")
            .to_string();

        Some(GeoLocationKey { country_code, city })
    }

    async fn find(&self, conn: &mut PgConnection, key: &Self::Key) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM geo_locations WHERE country_code = $1 AND city = $2 LIMIT 1",
        )
        .bind(&key.country_code)
        .bind(&key.city)
        .fetch_optional(&mut *conn)
        .await?;
        Ok(row.map(|(id,)| id))
    }

    async fn insert(
        &self,
        conn: &mut PgConnection,
        id: i64,
        key: &Self::Key,
    ) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "INSERT INTO geo_locations (id, country_code, city) VALUES ($1, $2, $3) \
             ON CONFLICT (country_code, city) DO NOTHING RETURNING id",
        )
        .bind(id)
        .bind(&key.country_code)
        .bind(&key.city)
        .fetch_optional(&mut *conn)
        .await?;
        Ok(row.map(|(id,)| id))
    }
}

// ── PartitionManager ────────────────────────────────────────────────

/// Manages monthly partition creation for redirect_events.
/// Uses in-memory cache to avoid redundant DDL calls.
struct PartitionManager {
    created: Mutex<HashSet<String>>,
}

impl PartitionManager {
    fn new() -> Self {
        Self {
            created: Mutex::new(HashSet::new()),
        }
    }

    async fn ensure(
        &self,
        conn: &mut PgConnection,
        ts: &chrono::NaiveDateTime,
    ) -> anyhow::Result<()> {
        let year = ts.date().year();
        let month = ts.date().month();
        let key = format!("{year}_{month:02}");

        {
            let cache = self.created.lock().unwrap();
            if cache.contains(&key) {
                return Ok(());
            }
        }

        let partition_name = format!("redirect_events_{key}");
        let start = format!("{year}-{month:02}-01");

        let (next_year, next_month) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
        let end = format!("{next_year}-{next_month:02}-01");

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {partition_name} \
             PARTITION OF redirect_events \
             FOR VALUES FROM ('{start}') TO ('{end}')"
        );

        sqlx::query(&sql).execute(&mut *conn).await?;
        self.created.lock().unwrap().insert(key);
        Ok(())
    }
}

// ── Standalone functions ────────────────────────────────────────────

/// Ensure the URL is stored in the local analytics reference table.
async fn ensure_url(conn: &mut PgConnection, url_id: i64, url: &str) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO urls (id, url) VALUES ($1, $2) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(url_id)
    .bind(url)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Insert a single redirect event row with Snowflake ID.
#[allow(clippy::too_many_arguments)]
async fn insert_event(
    conn: &mut PgConnection,
    event_id: i64,
    event: &super::RedirectEvent,
    referer_id: i64,
    user_agent_id: i64,
    referer_domain_id: i64,
    geo_location_id: i64,
    batch_id: i64,
) -> anyhow::Result<()> {
    let source_str = match event.source {
        DataSource::Cache => "cache",
        DataSource::Database => "database",
    };

    let ip_valid: Option<&str> = event
        .ip
        .as_deref()
        .filter(|s| s.parse::<std::net::IpAddr>().is_ok());

    sqlx::query(
        "INSERT INTO redirect_events \
         (id, url_id, event_timestamp, latency_micros, source, \
          referer_id, user_agent_id, referer_domain_id, geo_location_id, \
          ip, batch_id) \
         VALUES ($1, $2, $3, $4, $5::data_source, $6, $7, $8, $9, $10::inet, $11)",
    )
    .bind(event_id)
    .bind(event.url_id)
    .bind(event.timestamp)
    .bind(event.latency_micros as i64)
    .bind(source_str)
    .bind(referer_id)
    .bind(user_agent_id)
    .bind(referer_domain_id)
    .bind(geo_location_id)
    .bind(ip_valid)
    .bind(batch_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

// ── PostgresEventStorage ────────────────────────────────────────────

/// PostgreSQL implementation of EventStorage.
pub struct PostgresEventStorage {
    pool: PgPool,
    partitions: PartitionManager,
    snowflake: SnowflakeGenerator,
    referers: RefererResolver,
    user_agents: UserAgentResolver,
    referer_domains: RefererDomainResolver,
    geo_locations: GeoLocationResolver,
}

impl PostgresEventStorage {
    pub async fn new(config: &EventsConfig, geoip_path: Option<&str>) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&config.consumer.database_url)
            .await?;

        tracing::info!("Connected to PostgreSQL analytics database");

        let (reader, file_modified) = match geoip_path {
            Some(path) => {
                let reader = maxminddb::Reader::open_readfile(path)?;
                let modified = std::fs::metadata(path).and_then(|m| m.modified()).ok();
                tracing::info!(path, ?modified, "Loaded GeoIP database");
                (Some(reader), modified)
            }
            None => {
                tracing::warn!("No GeoIP database path configured, geo enrichment disabled");
                (None, None)
            }
        };

        Ok(Self {
            pool,
            partitions: PartitionManager::new(),
            snowflake: create_generator(1),
            referers: RefererResolver,
            user_agents: UserAgentResolver {
                parser: woothee::parser::Parser::new(),
            },
            referer_domains: RefererDomainResolver,
            geo_locations: GeoLocationResolver {
                geoip: Mutex::new(GeoIpState {
                    reader,
                    path: geoip_path.map(|s| s.to_string()),
                    file_modified,
                    last_check: Instant::now(),
                }),
            },
        })
    }
}

#[async_trait]
impl EventStorage for PostgresEventStorage {
    async fn ensure_schema(&self) -> anyhow::Result<()> {
        sqlx::migrate!("migrations/analytics")
            .run(&self.pool)
            .await?;
        tracing::info!("Database schema ready");
        Ok(())
    }

    async fn insert_batch(&self, batch: &EventBatch) -> anyhow::Result<usize> {
        match batch {
            EventBatch::Redirect {
                events, batch_id, ..
            } => self.insert_redirects(events, *batch_id).await,
        }
    }
}

impl PostgresEventStorage {
    async fn insert_redirects(
        &self,
        events: &[super::RedirectEvent],
        batch_id: i64,
    ) -> anyhow::Result<usize> {
        let mut tx = self.pool.begin().await?;

        // Ensure monthly partitions
        let mut seen_months = HashSet::new();
        for event in events {
            let key = (
                event.timestamp.date().year(),
                event.timestamp.date().month(),
            );
            if seen_months.insert(key) {
                self.partitions.ensure(&mut tx, &event.timestamp).await?;
            }
        }

        // Ensure URLs in reference table
        for event in events {
            ensure_url(&mut tx, event.url_id, &event.target_url).await?;
        }

        // Resolve references + insert events (all IDs via Snowflake)
        for event in events {
            let referer_id = resolve_or_create(
                &self.referers,
                &mut tx,
                &self.snowflake,
                event.referer.as_deref(),
            )
            .await?;
            let user_agent_id = resolve_or_create(
                &self.user_agents,
                &mut tx,
                &self.snowflake,
                event.user_agent.as_deref(),
            )
            .await?;
            let referer_domain_id = resolve_or_create(
                &self.referer_domains,
                &mut tx,
                &self.snowflake,
                event.referer.as_deref(),
            )
            .await?;
            let geo_location_id = resolve_or_create(
                &self.geo_locations,
                &mut tx,
                &self.snowflake,
                event.ip.as_deref(),
            )
            .await?;

            let event_id = self.snowflake.generate();
            insert_event(
                &mut tx,
                event_id,
                event,
                referer_id,
                user_agent_id,
                referer_domain_id,
                geo_location_id,
                batch_id,
            )
            .await?;
        }

        tx.commit().await?;
        Ok(events.len())
    }
}

// ── Domain normalization ────────────────────────────────────────────

/// Extract and normalize a domain from a URL.
/// - Extracts host portion
/// - Converts to lowercase
/// - Strips `www.` prefix
/// - Strips trailing dot
fn normalize_domain(url: &str) -> String {
    let host_start = url.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &url[host_start..];
    let host_end = rest.find(&['/', '?', '#', ':'][..]).unwrap_or(rest.len());
    let mut host = rest[..host_end].to_lowercase();

    if let Some(stripped) = host.strip_prefix("www.") {
        host = stripped.to_string();
    }
    if let Some(stripped) = host.strip_suffix('.') {
        host = stripped.to_string();
    }

    if host.is_empty() {
        "(unknown)".to_string()
    } else {
        host
    }
}

#[cfg(test)]
#[path = "storage_postgres_test.rs"]
mod storage_postgres_test;
