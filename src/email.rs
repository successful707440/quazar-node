use std::time::Duration;

use lettre::message::header::ContentType;
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub user: String,
    pub app_password: String,
    pub from_name: String,
}

impl SmtpConfig {
    pub fn from_env() -> Option<Self> {
        let user = std::env::var("GMAIL_USER").ok()?;
        let app_password = std::env::var("GMAIL_APP_PASSWORD").ok()?;
        if user.trim().is_empty() || app_password.trim().is_empty() {
            return None;
        }
        let from_name = std::env::var("GMAIL_FROM_NAME")
            .unwrap_or_else(|_| "Квазар".to_string());
        Some(Self {
            user: user.trim().to_string(),
            app_password: app_password.trim().to_string(),
            from_name,
        })
    }
}

fn send_verification_code_blocking(
    config: &SmtpConfig,
    to_email: &str,
    code: &str,
) -> Result<(), String> {
    let body = format!(
        "Здравствуйте!\n\n\
         Ваш код подтверждения регистрации в Квазаре: {code}\n\n\
         Код действителен 10 минут.\n\n\
         С уважением,\n\
         {from_name}\n\
         {from_email}\n\n\
         Если вы не запрашивали регистрацию — проигнорируйте это письмо.",
        from_name = config.from_name,
        from_email = config.user,
    );

    let from_mailbox: Mailbox = Mailbox::new(
        Some(config.from_name.clone()),
        config
            .user
            .parse()
            .map_err(|e| format!("invalid from address: {e}"))?,
    );

    let email = Message::builder()
        .from(from_mailbox)
        .to(to_email
            .parse()
            .map_err(|e| format!("invalid recipient address: {e}"))?)
        .subject("Код подтверждения регистрации в Квазаре")
        .header(ContentType::TEXT_PLAIN)
        .body(body)
        .map_err(|e| format!("failed to build email: {e}"))?;

    let creds = Credentials::new(config.user.clone(), config.app_password.clone());

    let mailer = SmtpTransport::starttls_relay("smtp.gmail.com")
        .map_err(|e| format!("SMTP relay error: {e}"))?
        .port(587)
        .credentials(creds)
        .timeout(Some(Duration::from_secs(20)))
        .build();

    mailer
        .send(&email)
        .map_err(|e| format!("failed to send email: {e}"))?;

    Ok(())
}

pub async fn send_verification_code(
    config: &SmtpConfig,
    to_email: &str,
    code: &str,
) -> Result<(), String> {
    let config = config.clone();
    let to_email = to_email.to_string();
    let code = code.to_string();

    let result = timeout(
        Duration::from_secs(25),
        tokio::task::spawn_blocking(move || {
            send_verification_code_blocking(&config, &to_email, &code)
        }),
    )
    .await
    .map_err(|_| "SMTP send timed out (check network or Gmail app password)".to_string())?
    .map_err(|e| format!("SMTP task failed: {e}"))?;

    result
}
