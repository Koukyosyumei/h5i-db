//! Canonical JSON for idempotency fingerprints.
//!
//! The fingerprint is blake3 of the request with `idempotency_key` removed.
//! Object keys are sorted and there is no insignificant whitespace, so two
//! serializations of the same request hash the same.

use serde::Serialize;
use serde_json::Value;

use crate::error::{Result, invalid};

pub fn request_fingerprint(request: &impl Serialize) -> Result<String> {
    let mut value = serde_json::to_value(request)
        .map_err(|error| crate::error::ServiceError::engine("metadata", error.to_string()))?;
    if let Some(object) = value.as_object_mut() {
        object.remove("idempotency_key");
    }
    Ok(blake3::hash(&canonical_bytes(&value)).to_hex().to_string())
}

pub fn require_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(invalid("a mutating call needs a non-empty idempotency key"));
    }
    Ok(())
}

fn canonical_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canon(&mut out, value);
    out
}

fn write_canon(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(number) => out.extend_from_slice(number.to_string().as_bytes()),
        Value::String(text) => {
            // serde_json's string encoding is the canonical escaping.
            let encoded = serde_json::to_string(text).expect("string encoding cannot fail");
            out.extend_from_slice(encoded.as_bytes());
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canon(out, item);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                let encoded = serde_json::to_string(key).expect("string encoding cannot fail");
                out.extend_from_slice(encoded.as_bytes());
                out.push(b':');
                write_canon(out, &map[*key]);
            }
            out.push(b'}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Sample {
        idempotency_key: String,
        table: String,
        n: u64,
    }

    #[test]
    fn the_key_is_not_part_of_the_fingerprint_and_key_order_is_not_either() {
        let left = Sample {
            idempotency_key: "a".into(),
            table: "trades".into(),
            n: 1,
        };
        let right = Sample {
            idempotency_key: "b".into(),
            table: "trades".into(),
            n: 1,
        };
        assert_eq!(
            request_fingerprint(&left).unwrap(),
            request_fingerprint(&right).unwrap()
        );
        let changed = Sample {
            idempotency_key: "a".into(),
            table: "trades".into(),
            n: 2,
        };
        assert_ne!(
            request_fingerprint(&left).unwrap(),
            request_fingerprint(&changed).unwrap()
        );
    }
}
