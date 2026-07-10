use std::sync::Arc;

use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::email::{send_citizenship_application, SmtpConfig};
use crate::response::{self, ApiResponse};
use crate::AppState;

const ADMIN_EMAIL: &str = "quazarvs@gmail.com";

#[derive(Deserialize)]
pub struct CitizenshipApplicationRequest {
    pub name: String,
    pub email: String,
    pub reason: String,
}

#[derive(Serialize)]
pub struct CitizenshipApplicationResponse {
    pub message: String,
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

pub async fn citizenship_application_handler(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<CitizenshipApplicationRequest>,
) -> impl IntoResponse {
    let name = req.name.trim();
    let email = req.email.trim();
    let reason = req.reason.trim();

    if name.is_empty() {
        return response::bad_request("Укажите имя");
    }
    if email.is_empty() {
        return response::bad_request("Укажите email");
    }
    if reason.is_empty() {
        return response::bad_request("Опишите, чем вы можете быть полезны");
    }
    if !is_valid_email(email) {
        return response::bad_request("Введите корректный email-адрес");
    }

    let Some(smtp) = SmtpConfig::from_env() else {
        tracing::error!("GMAIL_USER / GMAIL_APP_PASSWORD not configured");
        return response::internal_error(
            "Отправка email не настроена на сервере. Обратитесь к администратору.",
        );
    };

    if let Err(e) = send_citizenship_application(&smtp, ADMIN_EMAIL, name, email, reason).await {
        tracing::error!(applicant = %email, error = %e, "failed to send citizenship application email");
        return response::internal_error(
            "Не удалось отправить заявку. Попробуйте позже или напишите на quazarvs@gmail.com",
        );
    }

    tracing::info!(applicant = %email, name = %name, "citizenship application received");

    Json(ApiResponse::success(CitizenshipApplicationResponse {
        message: "Заявка получена".to_string(),
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_emails() {
        assert!(is_valid_email("user@example.com"));
    }

    #[test]
    fn rejects_invalid_emails() {
        assert!(!is_valid_email(""));
        assert!(!is_valid_email("not-an-email"));
    }
}
