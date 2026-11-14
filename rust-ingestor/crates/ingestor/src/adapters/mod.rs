use serde_json::Value;

use crate::domain::normalize_opt_string;

pub mod falabella;
pub mod vtex;

pub(crate) fn absolutize_url(base_url: &str, raw: Option<&str>) -> Option<String> {
    let cleaned = normalize_opt_string(raw)?;
    if cleaned.starts_with("http://") || cleaned.starts_with("https://") {
        Some(cleaned)
    } else if cleaned.starts_with('/') {
        Some(format!("{}{}", base_url.trim_end_matches('/'), cleaned))
    } else {
        Some(format!("{}/{}", base_url.trim_end_matches('/'), cleaned))
    }
}

pub(crate) fn value_as_string(val: Option<&Value>) -> Option<String> {
    match val? {
        Value::String(s) => normalize_opt_string(Some(s)),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

pub(crate) fn encode_url_segment(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len());
    for byte in input.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(*byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{:02X}", byte));
        }
    }
    encoded
}
