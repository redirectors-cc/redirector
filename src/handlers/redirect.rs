use crate::error::AppError;
use crate::events::dispatcher::EventDispatcher;
use crate::events::RedirectEvent;
use crate::services::{Cache, HashidDecoder, UrlResolver, UrlStorage};
use askama::Template;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{Html, IntoResponse, Redirect, Response},
};
use std::sync::Arc;
use std::time::Instant;

#[derive(Template)]
#[template(path = "interstitial.html")]
pub struct InterstitialTemplate {
    pub target_url: String,
    pub target_domain: String,
    pub delay_seconds: u32,
}

#[derive(Template)]
#[template(path = "not_found.html")]
pub struct NotFoundTemplate {}

pub struct RedirectState<H, C, S>
where
    H: HashidDecoder,
    C: Cache,
    S: UrlStorage,
{
    pub resolver: Arc<UrlResolver<H, C, S>>,
    pub delay_seconds: u32,
    pub dispatcher: EventDispatcher,
}

pub async fn redirect_handler<H, C, S>(
    State(state): State<Arc<RedirectState<H, C, S>>>,
    Path(hashid): Path<String>,
    headers: HeaderMap,
) -> Result<Response, AppError>
where
    H: HashidDecoder + 'static,
    C: Cache + 'static,
    S: UrlStorage + 'static,
{
    let start = Instant::now();
    metrics::counter!("redirect_requests").increment(1);

    let result = match state.resolver.resolve(&hashid).await {
        Ok(resolved) => {
            tracing::info!(
                hashid = %hashid,
                target = %resolved.full_url,
                "Redirect resolved"
            );

            // Record for dashboard
            crate::metrics::record_recent_redirect(hashid.clone(), resolved.full_url.clone());

            // Dispatch event (non-blocking, fire-and-forget)
            state.dispatcher.dispatch_redirect(RedirectEvent {
                url_id: resolved.id,
                target_url: resolved.full_url.clone(),
                timestamp: chrono::Utc::now().naive_utc(),
                latency_micros: start.elapsed().as_micros() as u64,
                source: resolved.source,
                referer: headers
                    .get("referer")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string()),
                user_agent: headers
                    .get("user-agent")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string()),
                ip: extract_client_ip(&headers),
            });

            // Direct HTTP redirect when delay is 0, otherwise show interstitial page
            if state.delay_seconds == 0 {
                Ok(Redirect::temporary(&resolved.full_url).into_response())
            } else {
                let template = InterstitialTemplate {
                    target_url: resolved.full_url,
                    target_domain: resolved.domain,
                    delay_seconds: state.delay_seconds,
                };

                Ok(Html(crate::minify_html_str(
                    &template
                        .render()
                        .map_err(|e| AppError::Internal(e.into()))?,
                ))
                .into_response())
            }
        }
        Err(AppError::NotFound | AppError::InvalidHashid) => {
            tracing::info!(hashid = %hashid, "URL not found");
            metrics::counter!("not_found_requests").increment(1);
            Err(AppError::NotFound)
        }
        Err(e) => Err(e),
    };

    let duration = start.elapsed();
    metrics::histogram!("request_duration_seconds").record(duration.as_secs_f64());
    crate::metrics::record_request(duration.as_micros() as u64);

    result
}

/// Extract client IP from headers (X-Forwarded-For > X-Real-IP > none).
fn extract_client_ip(headers: &HeaderMap) -> Option<String> {
    if let Some(forwarded) = headers.get("x-forwarded-for") {
        if let Ok(val) = forwarded.to_str() {
            // X-Forwarded-For can contain multiple IPs; take the first (client)
            return val.split(',').next().map(|s| s.trim().to_string());
        }
    }
    if let Some(real_ip) = headers.get("x-real-ip") {
        if let Ok(val) = real_ip.to_str() {
            return Some(val.trim().to_string());
        }
    }
    None
}

#[cfg(test)]
#[path = "redirect_test.rs"]
mod redirect_test;
