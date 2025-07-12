use axum::{body::Body, extract::State, http::{Method, StatusCode}, response::IntoResponse, routing::any, Router, RequestExt};
use axum::body::Body as AxumBody;
use axum::body::to_bytes;
use axum_server::Server;
use bytes::Bytes;
use reqwest::{Client, Response};
use serde::Deserialize;
use std::{collections::VecDeque, net::SocketAddr, str::FromStr, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{sync::{Mutex, oneshot}, time::sleep};

#[derive(Clone)]
struct AppState {
    queue: Arc<Mutex<VecDeque<ProxyRequest>>>,
    client: Client,
    auth_token: Arc<Mutex<AuthToken>>,
    rate_limit: Arc<Mutex<RateLimit>>, 
}

struct ProxyRequest {
    uri: String,
    method: String,
    body: Vec<u8>,
    reply: oneshot::Sender<(StatusCode, Vec<u8>)>,
}

#[derive(Clone, Default)]
struct RateLimit {
    remaining: u32,
    reset_time: u64,
}

#[derive(Default)]
struct AuthToken {
    token: String,
    expires_at: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tracing_subscriber::fmt::init();

    let state = AppState {
        queue: Arc::new(Mutex::new(VecDeque::new())),
        client: Client::builder().http2_prior_knowledge().build().unwrap(),
        auth_token: Arc::new(Mutex::new(AuthToken::default())),
        rate_limit: Arc::new(Mutex::new(RateLimit::default())),
    };

    let clone = state.clone();
    tokio::spawn(async move { process_queue(clone).await });

    let app = Router::new().route("/*path", any(proxy_handler)).with_state(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("Proxy listening on http://{}", addr);
    Server::bind(addr).serve(app.into_make_service()).await.unwrap();
}

async fn proxy_handler(State(state): State<AppState>, req: axum::http::Request<AxumBody>) -> impl IntoResponse {
    let uri = req.uri().to_string();
    let method = req.method().to_string();
    let body = to_bytes(req.into_body(), usize::MAX).await.unwrap_or_default().to_vec();
    let (tx, rx) = oneshot::channel();

    let proxy_req = ProxyRequest { uri, method, body, reply: tx };
    state.queue.lock().await.push_back(proxy_req);

    match rx.await {
        Ok((status, body)) => (status, body),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, b"Internal error".to_vec()),
    }
}

async fn process_queue(state: AppState) {
    loop {
        if let Some(req) = state.queue.lock().await.pop_front() {
            let mut rate = state.rate_limit.lock().await;

            let now = current_unix();
            if rate.remaining == 0 && now < rate.reset_time {
                let wait = Duration::from_secs(rate.reset_time - now);
                drop(rate);
                sleep(wait).await;
                continue;
            }
            rate.remaining = rate.remaining.saturating_sub(1);
            drop(rate);

            let token = ensure_token(&state).await;
            let url = format!("https://api.twitch.tv{}", req.uri);
            let method = Method::from_str(&req.method).unwrap_or(Method::GET);
            let r = state.client.request(method, &url)
                .header("Authorization", format!("Bearer {}", token))
                .header("Client-ID", "TU_CLIENT_ID")
                .body(req.body.clone());

            match r.send().await {
                Ok(resp) => {
                    update_rate_limit(&state, &resp).await;
                    let status = resp.status();
                    let body = resp.bytes().await.unwrap_or_default().to_vec();
                    let _ = req.reply.send((status, body));
                }
                Err(err) => {
                    tracing::error!("Request to Twitch failed: {}", err);
                    let _ = req.reply.send((StatusCode::BAD_GATEWAY, b"Twitch unreachable".to_vec()));
                }
            }
        } else {
            sleep(Duration::from_millis(25)).await;
        }
    }
}

async fn ensure_token(state: &AppState) -> String {
    let mut auth = state.auth_token.lock().await;
    let now = current_unix();
    if !auth.token.is_empty() && now < auth.expires_at {
        return auth.token.clone();
    }

    #[derive(Deserialize)]
    struct TokenResp {
        access_token: String,
        expires_in: u64,
    }

    let res = state.client.post("https://id.twitch.tv/oauth2/token")
        .query(&[
            ("client_id", "TU_CLIENT_ID"),
            ("client_secret", "TU_CLIENT_SECRET"),
            ("grant_type", "client_credentials")
        ])
        .send().await.unwrap();

    let json: TokenResp = res.json().await.unwrap();
    auth.token = json.access_token;
    auth.expires_at = now + json.expires_in;
    auth.token.clone()
}

async fn update_rate_limit(state: &AppState, resp: &Response) {
    let mut rate = state.rate_limit.lock().await;
    if let Some(r) = resp.headers().get("Ratelimit-Remaining") {
        rate.remaining = r.to_str().unwrap_or("0").parse().unwrap_or(0);
    }
    if let Some(r) = resp.headers().get("Ratelimit-Reset") {
        rate.reset_time = r.to_str().unwrap_or("0").parse().unwrap_or(0);
    }
}

fn current_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
