use axum::{
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ApiResponse {
    pub fn success<T: Serialize>(data: T) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(serde_json::to_value(data).unwrap_or(Value::Null)),
            error: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error: Some(message.into()),
        }
    }
}

pub fn decode_data<T: for<'de> Deserialize<'de>>(response: &ApiResponse) -> Option<T> {
    response
        .data
        .as_ref()
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

pub fn err_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(ApiResponse::error(message))).into_response()
}

pub fn unauthorized(message: impl Into<String>) -> Response {
    err_response(StatusCode::UNAUTHORIZED, message)
}

pub fn forbidden(message: impl Into<String>) -> Response {
    err_response(StatusCode::FORBIDDEN, message)
}

pub fn bad_request(message: impl Into<String>) -> Response {
    err_response(StatusCode::BAD_REQUEST, message)
}

pub fn internal_error(message: impl Into<String>) -> Response {
    err_response(StatusCode::INTERNAL_SERVER_ERROR, message)
}
