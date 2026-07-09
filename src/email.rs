use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

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

pub async fn send_verification_code(
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

    let email = Message::builder()
        .from(
            format!("{} <{}>", config.from_name, config.user)
                .parse()
                .map_err(|e| format!("invalid from address: {e}"))?,
        )
        .to(to_email
            .parse()
            .map_err(|e| format!("invalid recipient address: {e}"))?)
        .subject("Код подтверждения регистрации в Квазаре")
        .header(ContentType::TEXT_PLAIN)
        .body(body)
        .map_err(|e| format!("failed to build email: {e}"))?;

    let creds = Credentials::new(config.user.clone(), config.app_password.clone());

    let mailer = AsyncSmtpTransport::<Tokio1Executor>::relay("smtp.gmail.com")
        .map_err(|e| format!("SMTP relay error: {e}"))?
        .credentials(creds)
        .build();

    mailer
        .send(email)
        .await
        .map_err(|e| format!("failed to send email: {e}"))?;

    Ok(())
}
