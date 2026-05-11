use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, TokenData, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,       // User address
    pub exp: i64,          // Expiration time
    pub iat: i64,          // Issued at
}

pub struct JwtManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    expiry_seconds: u64,
}

/// Standalone function to validate a JWT token
pub fn validate_token(token: &str, secret: &str) -> anyhow::Result<Claims> {
    let decoding_key = DecodingKey::from_secret(secret.as_bytes());
    let token_data: TokenData<Claims> = decode(token, &decoding_key, &Validation::default())?;
    Ok(token_data.claims)
}

impl JwtManager {
    pub fn new(secret: &str, expiry_seconds: u64) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            expiry_seconds,
        }
    }

    pub fn generate_token(&self, address: &str) -> anyhow::Result<String> {
        let now = Utc::now();
        let exp = now + Duration::seconds(self.expiry_seconds as i64);

        let claims = Claims {
            sub: address.to_lowercase(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
        };

        let token = encode(&Header::default(), &claims, &self.encoding_key)?;
        Ok(token)
    }

    pub fn verify_token(&self, token: &str) -> anyhow::Result<Claims> {
        let token_data: TokenData<Claims> =
            decode(token, &self.decoding_key, &Validation::default())?;
        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_roundtrip() {
        let manager = JwtManager::new("test_secret", 3600);
        let address = "0x1234567890abcdef1234567890abcdef12345678";

        let token = manager.generate_token(address).unwrap();
        let claims = manager.verify_token(&token).unwrap();

        assert_eq!(claims.sub, address.to_lowercase());
    }
}
