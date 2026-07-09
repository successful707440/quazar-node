use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::{
    extract::{ConnectInfo, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::email::{send_verification_code, SmtpConfig};
use crate::response::{self, ApiResponse};
use crate::AppState;

const CODE_TTL: Duration = Duration::from_secs(10 * 60);
const RESEND_COOLDOWN: Duration = Duration::from_secs(60);
const MAX_SENDS_PER_IP_PER_HOUR: u32 = 10;

#[derive(Clone)]
struct PendingCode {
    code: String,
    expires_at: Instant,
    last_sent_at: Instant,
}

static CODES: LazyLock<Mutex<HashMap<String, PendingCode>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static IP_SEND_COUNTS: LazyLock<Mutex<HashMap<String, (u32, Instant)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

fn is_valid_email(email: &str) -> bool {
    let email = email.trim();
    if email.len() < 5 || email.len() > 254 {
        return false;
    }
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && !domain.is_empty()
        && domain.contains('.')
        && !email.contains(char::is_whitespace)
}

fn generate_code() -> String {
    format!("{:06}", uuid::Uuid::new_v4().as_u128() % 1_000_000)
}

fn purge_expired_codes(codes: &mut HashMap<String, PendingCode>) {
    let now = Instant::now();
    codes.retain(|_, entry| entry.expires_at > now);
}

fn check_ip_rate_limit(ip: &str) -> Result<(), String> {
    let mut counts = IP_SEND_COUNTS.lock().expect("ip send counts lock");
    let now = Instant::now();
    let hour = Duration::from_secs(3600);

    counts.retain(|_, (_, started_at)| now.duration_since(*started_at) < hour);

    let entry = counts.entry(ip.to_string()).or_insert((0, now));
    if now.duration_since(entry.1) >= hour {
        *entry = (0, now);
    }
    if entry.0 >= MAX_SENDS_PER_IP_PER_HOUR {
        return Err("Слишком много запросов. Попробуйте позже.".to_string());
    }
    entry.0 += 1;
    Ok(())
}

fn client_ip(connect_info: Option<&ConnectInfo<SocketAddr>>) -> String {
    connect_info
        .map(|ConnectInfo(addr)| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[derive(Deserialize)]
pub struct SendCodeRequest {
    pub email: String,
}

#[derive(Serialize)]
pub struct SendCodeResponse {
    pub message: String,
    pub expires_in_secs: u64,
}

#[derive(Deserialize)]
pub struct VerifyCodeRequest {
    pub email: String,
    pub code: String,
}

#[derive(Serialize)]
pub struct VerifyCodeResponse {
    pub message: String,
}

pub async fn send_code_handler(
    State(_state): State<Arc<AppState>>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    Json(req): Json<SendCodeRequest>,
) -> impl IntoResponse {
    let email = normalize_email(&req.email);
    if !is_valid_email(&email) {
        return response::bad_request("Введите корректный email-адрес");
    }

    let ip = client_ip(connect_info.as_ref());
    if let Err(message) = check_ip_rate_limit(&ip) {
        return response::err_response(axum::http::StatusCode::TOO_MANY_REQUESTS, message);
    }

    let Some(smtp) = SmtpConfig::from_env() else {
        tracing::error!("GMAIL_USER / GMAIL_APP_PASSWORD not configured");
        return response::internal_error(
            "Отправка email не настроена на сервере. Обратитесь к администратору.",
        );
    };

    let now = Instant::now();
    {
        let mut codes = CODES.lock().expect("verification codes lock");
        purge_expired_codes(&mut codes);
        if let Some(existing) = codes.get(&email) {
            if now.duration_since(existing.last_sent_at) < RESEND_COOLDOWN {
                return response::bad_request(
                    "Код уже отправлен. Подождите минуту перед повторной отправкой.",
                );
            }
        }
    }

    let code = generate_code();

    if let Err(e) = send_verification_code(&smtp, &email, &code).await {
        tracing::error!(to = %email, error = %e, "failed to send verification email");
        return response::internal_error(
            "Не удалось отправить письмо. Попробуйте позже или напишите на quazarvs@gmail.com",
        );
    }

    {
        let mut codes = CODES.lock().expect("verification codes lock");
        codes.insert(
            email.clone(),
            PendingCode {
                code: code.clone(),
                expires_at: now + CODE_TTL,
                last_sent_at: now,
            },
        );
    }

    tracing::info!(to = %email, client_ip = %ip, "verification code sent");

    Json(ApiResponse::success(SendCodeResponse {
        message: "Код отправлен на ваш email".to_string(),
        expires_in_secs: CODE_TTL.as_secs(),
    }))
    .into_response()
}

pub async fn verify_code_handler(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<VerifyCodeRequest>,
) -> impl IntoResponse {
    let email = normalize_email(&req.email);
    let code = req.code.trim();

    if !is_valid_email(&email) {
        return response::bad_request("Введите корректный email-адрес");
    }
    if !code.chars().all(|c| c.is_ascii_digit()) || code.len() != 6 {
        return response::bad_request("Код должен состоять из 6 цифр");
    }

    let mut codes = CODES.lock().expect("verification codes lock");
    purge_expired_codes(&mut codes);

    let Some(pending) = codes.get(&email) else {
        return response::bad_request("Код не найден. Запросите новый код.");
    };

    if pending.expires_at <= Instant::now() {
        codes.remove(&email);
        return response::bad_request("Код истёк. Запросите новый код.");
    }

    if pending.code != code {
        return response::bad_request("Неверный код. Проверьте письмо и попробуйте снова.");
    }

    codes.remove(&email);

    Json(ApiResponse::success(VerifyCodeResponse {
        message: "Email подтверждён".to_string(),
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_emails() {
        assert!(is_valid_email("user@example.com"));
        assert!(is_valid_email("  User@Example.COM  "));
    }

    #[test]
    fn rejects_invalid_emails() {
        assert!(!is_valid_email(""));
        assert!(!is_valid_email("not-an-email"));
        assert!(!is_valid_email("@example.com"));
        assert!(!is_valid_email("user@"));
    }

    #[test]
    fn generate_code_is_six_digits() {
        let code = generate_code();
        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|c| c.is_ascii_digit()));
    }
}
