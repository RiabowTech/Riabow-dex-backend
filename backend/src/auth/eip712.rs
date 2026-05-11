#![allow(dead_code)]
use ethers::abi::Token;
use ethers::types::{Address, Signature, H256, U256};
use ethers::utils::keccak256;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::OnceLock;

/// EIP-712 Type Hashes
pub const LOGIN_TYPEHASH: &str = "Login(address wallet,uint256 nonce,uint256 timestamp)";
pub const CREATE_ORDER_TYPEHASH: &str = "CreateOrder(address wallet,string symbol,string side,string orderType,string price,string amount,uint32 leverage,uint256 timestamp)";
pub const CANCEL_ORDER_TYPEHASH: &str = "CancelOrder(address wallet,string orderId,uint256 timestamp)";
pub const BATCH_CANCEL_TYPEHASH: &str = "BatchCancelOrders(address wallet,string orderIds,uint256 timestamp)";
pub const CREATE_REFERRAL_TYPEHASH: &str = "CreateReferralCode(address wallet,uint256 timestamp)";
pub const BIND_REFERRAL_TYPEHASH: &str = "BindReferralCode(address wallet,string code,uint256 timestamp)";
pub const WS_AUTH_TYPEHASH: &str = "WebSocketAuth(address wallet,uint256 timestamp)";

/// Global EIP-712 domain configuration (initialized from AppConfig at startup)
static DOMAIN: OnceLock<EIP712Domain> = OnceLock::new();

/// Initialize the global EIP-712 domain from configuration
/// Should be called once at application startup
pub fn init_domain(domain_name: &str, chain_id: u64, verifying_contract: &str) {
    let _ = DOMAIN.set(EIP712Domain {
        name: domain_name.to_string(),
        version: "1".to_string(),
        chain_id,
        verifying_contract: verifying_contract.to_string(),
    });
    tracing::info!(
        "EIP-712 domain initialized: name={}, chainId={}, verifyingContract={}",
        domain_name,
        chain_id,
        verifying_contract
    );
}

/// Get the global EIP-712 domain
pub fn get_domain() -> &'static EIP712Domain {
    DOMAIN.get().expect("EIP-712 domain not initialized. Call init_domain() at startup.")
}

/// EIP-712 Domain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EIP712Domain {
    pub name: String,
    pub version: String,
    pub chain_id: u64,
    pub verifying_contract: String,
}

impl EIP712Domain {
    /// Create a new EIP712Domain with custom values
    pub fn new(chain_id: u64, verifying_contract: &str) -> Self {
        Self {
            name: "ZTDX".to_string(),
            version: "1".to_string(),
            chain_id,
            verifying_contract: verifying_contract.to_string(),
        }
    }
}

/// Login message for EIP-712 signature verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginMessage {
    pub wallet: String,
    pub nonce: u64,
    pub timestamp: u64,
}

impl LoginMessage {
    /// Compute the struct hash for this login message according to EIP-712
    pub fn struct_hash(&self) -> H256 {
        let type_hash = keccak256(LOGIN_TYPEHASH.as_bytes());
        let wallet_address = Address::from_str(&self.wallet).unwrap_or_default();

        let encoded = ethers::abi::encode(&[
            Token::FixedBytes(type_hash.to_vec()),
            Token::Address(wallet_address),
            Token::Uint(U256::from(self.nonce)),
            Token::Uint(U256::from(self.timestamp)),
        ]);

        H256::from(keccak256(&encoded))
    }
}

/// Create Order message for EIP-712 signature verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrderMessage {
    pub wallet: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub amount: String,
    pub leverage: u32,
    pub timestamp: u64,
}

impl CreateOrderMessage {
    pub fn struct_hash(&self) -> H256 {
        let type_hash = keccak256(CREATE_ORDER_TYPEHASH.as_bytes());
        let wallet_address = Address::from_str(&self.wallet).unwrap_or_default();

        let encoded = ethers::abi::encode(&[
            Token::FixedBytes(type_hash.to_vec()),
            Token::Address(wallet_address),
            Token::FixedBytes(keccak256(self.symbol.as_bytes()).to_vec()),
            Token::FixedBytes(keccak256(self.side.as_bytes()).to_vec()),
            Token::FixedBytes(keccak256(self.order_type.as_bytes()).to_vec()),
            Token::FixedBytes(keccak256(self.price.as_bytes()).to_vec()),
            Token::FixedBytes(keccak256(self.amount.as_bytes()).to_vec()),
            Token::Uint(U256::from(self.leverage)),
            Token::Uint(U256::from(self.timestamp)),
        ]);

        H256::from(keccak256(&encoded))
    }
}

/// Cancel Order message for EIP-712 signature verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelOrderMessage {
    pub wallet: String,
    pub order_id: String,
    pub timestamp: u64,
}

impl CancelOrderMessage {
    pub fn struct_hash(&self) -> H256 {
        let type_hash = keccak256(CANCEL_ORDER_TYPEHASH.as_bytes());
        let wallet_address = Address::from_str(&self.wallet).unwrap_or_default();

        let encoded = ethers::abi::encode(&[
            Token::FixedBytes(type_hash.to_vec()),
            Token::Address(wallet_address),
            Token::FixedBytes(keccak256(self.order_id.as_bytes()).to_vec()),
            Token::Uint(U256::from(self.timestamp)),
        ]);

        H256::from(keccak256(&encoded))
    }
}

/// Batch Cancel Orders message for EIP-712 signature verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCancelMessage {
    pub wallet: String,
    pub order_ids: String, // Comma-separated list of order IDs
    pub timestamp: u64,
}

impl BatchCancelMessage {
    pub fn struct_hash(&self) -> H256 {
        let type_hash = keccak256(BATCH_CANCEL_TYPEHASH.as_bytes());
        let wallet_address = Address::from_str(&self.wallet).unwrap_or_default();

        let encoded = ethers::abi::encode(&[
            Token::FixedBytes(type_hash.to_vec()),
            Token::Address(wallet_address),
            Token::FixedBytes(keccak256(self.order_ids.as_bytes()).to_vec()),
            Token::Uint(U256::from(self.timestamp)),
        ]);

        H256::from(keccak256(&encoded))
    }
}

/// Create Referral Code message for EIP-712 signature verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReferralMessage {
    pub wallet: String,
    pub timestamp: u64,
}

impl CreateReferralMessage {
    pub fn struct_hash(&self) -> H256 {
        let type_hash = keccak256(CREATE_REFERRAL_TYPEHASH.as_bytes());
        let wallet_address = Address::from_str(&self.wallet).unwrap_or_default();

        let encoded = ethers::abi::encode(&[
            Token::FixedBytes(type_hash.to_vec()),
            Token::Address(wallet_address),
            Token::Uint(U256::from(self.timestamp)),
        ]);

        H256::from(keccak256(&encoded))
    }
}

/// Bind Referral Code message for EIP-712 signature verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindReferralMessage {
    pub wallet: String,
    pub code: String,
    pub timestamp: u64,
}

impl BindReferralMessage {
    pub fn struct_hash(&self) -> H256 {
        let type_hash = keccak256(BIND_REFERRAL_TYPEHASH.as_bytes());
        let wallet_address = Address::from_str(&self.wallet).unwrap_or_default();

        let encoded = ethers::abi::encode(&[
            Token::FixedBytes(type_hash.to_vec()),
            Token::Address(wallet_address),
            Token::FixedBytes(keccak256(self.code.as_bytes()).to_vec()),
            Token::Uint(U256::from(self.timestamp)),
        ]);

        H256::from(keccak256(&encoded))
    }
}

/// WebSocket Auth message for EIP-712 signature verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketAuthMessage {
    pub wallet: String,
    pub timestamp: u64,
}

impl WebSocketAuthMessage {
    pub fn struct_hash(&self) -> H256 {
        let type_hash = keccak256(WS_AUTH_TYPEHASH.as_bytes());
        let wallet_address = Address::from_str(&self.wallet).unwrap_or_default();

        let encoded = ethers::abi::encode(&[
            Token::FixedBytes(type_hash.to_vec()),
            Token::Address(wallet_address),
            Token::Uint(U256::from(self.timestamp)),
        ]);

        H256::from(keccak256(&encoded))
    }
}

/// Withdraw message for signature verification (not yet implemented)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawMessage {
    pub token: String,
    pub amount: String,
    pub to_address: String,
    pub timestamp: u64,
}

/// Verify EIP-712 typed data signature for login
///
/// This follows the EIP-712 standard:
/// - Domain separator: keccak256(EIP712Domain type hash + encoded domain)
/// - Message hash: keccak256("\x19\x01" || domainSeparator || structHash)
pub fn verify_login_signature(
    login_msg: &LoginMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let domain = get_domain();
    let struct_hash = login_msg.struct_hash();

    verify_typed_signature(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for login with detailed debug info
pub fn verify_login_signature_with_debug(
    login_msg: &LoginMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<VerifyResult> {
    let domain = get_domain();
    let struct_hash = login_msg.struct_hash();

    verify_typed_signature_with_debug(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for creating an order
pub fn verify_create_order_signature(
    msg: &CreateOrderMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let domain = get_domain();
    let struct_hash = msg.struct_hash();
    verify_typed_signature(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for creating an order with debug info
pub fn verify_create_order_signature_with_debug(
    msg: &CreateOrderMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<VerifyResult> {
    let domain = get_domain();
    let struct_hash = msg.struct_hash();
    verify_typed_signature_with_debug(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for canceling an order
pub fn verify_cancel_order_signature(
    msg: &CancelOrderMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let domain = get_domain();
    let struct_hash = msg.struct_hash();
    verify_typed_signature(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for batch canceling orders
pub fn verify_batch_cancel_signature(
    msg: &BatchCancelMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let domain = get_domain();
    let struct_hash = msg.struct_hash();
    verify_typed_signature(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for creating a referral code
pub fn verify_create_referral_signature(
    msg: &CreateReferralMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let domain = get_domain();
    let struct_hash = msg.struct_hash();
    verify_typed_signature(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for binding a referral code
pub fn verify_bind_referral_signature(
    msg: &BindReferralMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let domain = get_domain();
    let struct_hash = msg.struct_hash();
    verify_typed_signature(domain, struct_hash, signature, expected_address)
}

/// Verify EIP-712 typed data signature for WebSocket authentication
pub fn verify_ws_auth_signature(
    msg: &WebSocketAuthMessage,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let domain = get_domain();
    let struct_hash = msg.struct_hash();
    verify_typed_signature(domain, struct_hash, signature, expected_address)
}

/// Result of EIP-712 signature verification with debug info
#[derive(Debug)]
pub struct VerifyResult {
    pub is_valid: bool,
    pub recovered_address: String,
    pub expected_address: String,
    pub domain_separator: String,
    pub struct_hash: String,
    pub message_hash: String,
}

/// Verify any EIP-712 typed data signature with detailed debug info
pub fn verify_typed_signature_with_debug(
    domain: &EIP712Domain,
    struct_hash: H256,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<VerifyResult> {
    // Compute domain separator
    let domain_separator = compute_domain_separator(domain);

    // Compute final hash: keccak256("\x19\x01" || domainSeparator || hashStruct)
    let mut data = Vec::with_capacity(66);
    data.extend_from_slice(&[0x19, 0x01]);
    data.extend_from_slice(domain_separator.as_bytes());
    data.extend_from_slice(struct_hash.as_bytes());

    let message_hash = H256::from(keccak256(&data));

    // Parse signature and recover
    let sig = Signature::from_str(signature.trim_start_matches("0x"))?;
    let recovered = sig.recover(message_hash)?;
    let expected = Address::from_str(expected_address)?;

    Ok(VerifyResult {
        is_valid: recovered == expected,
        recovered_address: format!("{:?}", recovered),
        expected_address: format!("{:?}", expected),
        domain_separator: format!("{:?}", domain_separator),
        struct_hash: format!("{:?}", struct_hash),
        message_hash: format!("{:?}", message_hash),
    })
}

/// Verify any EIP-712 typed data signature
pub fn verify_typed_signature(
    domain: &EIP712Domain,
    struct_hash: H256,
    signature: &str,
    expected_address: &str,
) -> anyhow::Result<bool> {
    let result = verify_typed_signature_with_debug(domain, struct_hash, signature, expected_address)?;
    Ok(result.is_valid)
}

/// Get the EIP-712 typed data structure for frontend signing
/// Returns the complete typed data object that can be used with eth_signTypedData_v4
pub fn get_login_typed_data(wallet: &str, nonce: u64, timestamp: u64) -> serde_json::Value {
    let domain = get_domain();

    serde_json::json!({
        "types": {
            "EIP712Domain": [
                { "name": "name", "type": "string" },
                { "name": "version", "type": "string" },
                { "name": "chainId", "type": "uint256" },
                { "name": "verifyingContract", "type": "address" }
            ],
            "Login": [
                { "name": "wallet", "type": "address" },
                { "name": "nonce", "type": "uint256" },
                { "name": "timestamp", "type": "uint256" }
            ]
        },
        "primaryType": "Login",
        "domain": {
            "name": domain.name,
            "version": domain.version,
            "chainId": domain.chain_id,
            "verifyingContract": domain.verifying_contract
        },
        "message": {
            "wallet": wallet,
            "nonce": nonce.to_string(),
            "timestamp": timestamp.to_string()
        }
    })
}

/// Get the EIP-712 typed data structure for create order signing
/// Returns the complete typed data object that can be used with eth_signTypedData_v4
pub fn get_create_order_typed_data(msg: &CreateOrderMessage) -> serde_json::Value {
    let domain = get_domain();

    serde_json::json!({
        "types": {
            "EIP712Domain": [
                { "name": "name", "type": "string" },
                { "name": "version", "type": "string" },
                { "name": "chainId", "type": "uint256" },
                { "name": "verifyingContract", "type": "address" }
            ],
            "CreateOrder": [
                { "name": "wallet", "type": "address" },
                { "name": "symbol", "type": "string" },
                { "name": "side", "type": "string" },
                { "name": "orderType", "type": "string" },
                { "name": "price", "type": "string" },
                { "name": "amount", "type": "string" },
                { "name": "leverage", "type": "uint32" },
                { "name": "timestamp", "type": "uint256" }
            ]
        },
        "primaryType": "CreateOrder",
        "domain": {
            "name": domain.name,
            "version": domain.version,
            "chainId": domain.chain_id,
            "verifyingContract": domain.verifying_contract
        },
        "message": {
            "wallet": msg.wallet,
            "symbol": msg.symbol,
            "side": msg.side,
            "orderType": msg.order_type,
            "price": msg.price,
            "amount": msg.amount,
            "leverage": msg.leverage,
            "timestamp": msg.timestamp.to_string()
        }
    })
}

fn compute_domain_separator(domain: &EIP712Domain) -> H256 {
    let type_hash = keccak256(
        "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );

    let name_hash = keccak256(domain.name.as_bytes());
    let version_hash = keccak256(domain.version.as_bytes());
    let verifying_contract = Address::from_str(&domain.verifying_contract)
        .unwrap_or_default();

    // Properly encode all fields according to EIP-712 spec
    // Address must be encoded as 32 bytes (left-padded with zeros)
    let encoded = ethers::abi::encode(&[
        Token::FixedBytes(type_hash.to_vec()),
        Token::FixedBytes(name_hash.to_vec()),
        Token::FixedBytes(version_hash.to_vec()),
        Token::Uint(domain.chain_id.into()),
        Token::Address(verifying_contract),
    ]);

    H256::from(keccak256(&encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_separator() {
        let domain = EIP712Domain::new(421614, "0xFDe43f8e6e082975d246844DEF4fE8E704403d43");
        let separator = compute_domain_separator(&domain);
        assert!(!separator.is_zero());
    }
}
