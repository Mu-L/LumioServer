//! Allocation-bound admission. Contract source: LumioGameEngine@23401e178fdf346a0361b51a1ff881daf4d42554,
//! engine/wire/account-port-v1.json. No online nonce-consumption table: v1 is
//! explicitly a bounded bearer-replay policy, not a single-use-ticket protocol.
use super::admission::{is_bot_namespace, AdmissionPayload};
use super::crypto::{
    base64url_decode, base64url_encode, verify_typed, BinReader, ADMISSION_PAYLOAD_TYPE,
    ADMISSION_PAYLOAD_VERSION, ADMISSION_TRUST_DOMAIN, NONCE_LEN, SIGNATURE_LEN,
};
use lumio_host_runtime::{HostClock, SharedClock};
use serde::Deserialize;

const MAX_CREDENTIAL_BYTES: usize = 16_384;
const UNBOUND: &str = "__unbound__";

/// Trusted deployment/allocation registry data; never constructed from a client frame.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AllocationContext {
    pub server_audience: String,
    pub game_id: String,
    pub game_release_id: String,
    pub contract_id: String,
    pub room_id: String,
    pub allocation_id: String,
}
impl AllocationContext {
    fn fields(&self) -> [&str; 6] {
        [
            &self.server_audience,
            &self.game_id,
            &self.game_release_id,
            &self.contract_id,
            &self.room_id,
            &self.allocation_id,
        ]
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.fields().iter().any(|s| {
            s.is_empty()
                || *s == UNBOUND
                || s.len() > 256
                || !s.is_ascii()
                || s.bytes().any(|b| b.is_ascii_control())
        }) {
            return Err("admission_binding_mismatch".to_owned());
        }
        Ok(())
    }
}

/// Proof has private fields; the socket cannot invent an account or a room.
#[derive(Clone)]
pub(crate) struct VerifiedAdmission {
    pub(crate) payload: AdmissionPayload,
    pub(crate) room_id: String,
}

/// Immutable key/allocation snapshot with an advancing, testable clock.
#[derive(Clone)]
pub struct BoundAdmissionVerifier {
    pub(crate) allocation: AllocationContext,
    pub(crate) key_id: u8,
    pub(crate) public_key: Vec<u8>,
    pub(crate) clock: SharedClock,
    pub(crate) unix_origin: u64,
    clock_origin_ms: u64,
}
impl BoundAdmissionVerifier {
    pub fn new(
        allocation: AllocationContext,
        key_id: u8,
        public_key: Vec<u8>,
        clock: SharedClock,
        unix_origin: u64,
    ) -> Result<Self, String> {
        allocation.validate()?;
        if public_key.len() != 32 {
            return Err("admission key must contain 32 bytes".to_owned());
        }
        let clock_origin_ms = clock.now_ms();
        Ok(Self {
            allocation,
            key_id,
            public_key,
            clock,
            unix_origin,
            clock_origin_ms,
        })
    }
    pub(crate) fn now(&self) -> u64 {
        self.unix_origin
            .saturating_add(self.clock.now_ms().saturating_sub(self.clock_origin_ms) / 1000)
    }
    pub(crate) fn verify(&self, wire: &str) -> Result<VerifiedAdmission, String> {
        let malformed = || "admission_credential_malformed".to_owned();
        if wire.is_empty() || wire.len() > MAX_CREDENTIAL_BYTES {
            return Err(malformed());
        }
        let bytes = base64url_decode(wire).ok_or_else(malformed)?;
        // Reject alternate encodings and padding rather than allowing aliases.
        if base64url_encode(&bytes) != wire || bytes.len() <= SIGNATURE_LEN {
            return Err(malformed());
        }
        let (payload_bytes, signature) = bytes.split_at(bytes.len() - SIGNATURE_LEN);
        let mut reader = BinReader::new(payload_bytes);
        if reader.read_u16() != Some(ADMISSION_PAYLOAD_VERSION) {
            return Err(malformed());
        }
        let key_id = reader.read_u8().ok_or_else(malformed)?;
        let account_id = reader.read_ascii().ok_or_else(malformed)?;
        let login_name = reader.read_ascii().ok_or_else(malformed)?;
        let bot = reader.read_u8().ok_or_else(malformed)?;
        let issued_at = reader.read_u64().ok_or_else(malformed)?;
        let expires_at = reader.read_u64().ok_or_else(malformed)?;
        let _nonce = reader.read_fixed(NONCE_LEN).ok_or_else(malformed)?;
        let mut binding = Vec::with_capacity(6);
        for _ in 0..6 {
            binding.push(reader.read_ascii().ok_or_else(malformed)?);
        }
        if reader.remaining() != 0 || bot > 1 || expires_at < issued_at {
            return Err(malformed());
        }
        if key_id != self.key_id
            || !verify_typed(
                &self.public_key,
                ADMISSION_TRUST_DOMAIN,
                ADMISSION_PAYLOAD_TYPE,
                payload_bytes,
                signature,
            )
        {
            return Err("admission_credential_invalid_signature".to_owned());
        }
        if self.now() > expires_at {
            return Err("admission_credential_expired".to_owned());
        }
        if binding.iter().all(|s| s == UNBOUND) {
            return Err("admission_credential_unbound".to_owned());
        }
        if binding
            .iter()
            .zip(self.allocation.fields())
            .any(|(actual, expected)| actual.is_empty() || actual == UNBOUND || actual != expected)
        {
            return Err("admission_binding_mismatch".to_owned());
        }
        if account_id.len() != 37
            || !account_id.starts_with("acct_")
            || !account_id[5..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(malformed());
        }
        if !(3..=32).contains(&login_name.len())
            || !login_name.as_bytes()[0].is_ascii_alphabetic()
            || !login_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(malformed());
        }
        if is_bot_namespace(&login_name) && bot == 0 {
            return Err("bot_namespace_admission_forbidden".to_owned());
        }
        Ok(VerifiedAdmission {
            payload: AdmissionPayload {
                key_id,
                account_id,
                login_name,
                bot_tool_context: bot == 1,
                issued_at,
                expires_at,
            },
            room_id: self.allocation.room_id.clone(),
        })
    }
}

/// Test issuer, absent from normal library builds. No private key enters Host config.
#[cfg(any(test, feature = "test-harness"))]
pub fn issue_bound_test_credential(
    seed: &[u8; 32],
    allocation: &AllocationContext,
    expires_at: u64,
) -> String {
    use super::crypto::{sign_typed, BinWriter};
    let mut w = BinWriter::new();
    w.write_u16(1);
    w.write_u8(1);
    w.write_ascii("acct_0123456789abcdef0123456789abcdef");
    w.write_ascii("Player01");
    w.write_u8(0);
    w.write_u64(1000);
    w.write_u64(expires_at);
    w.write_fixed(&[1; NONCE_LEN]);
    for field in allocation.fields() {
        w.write_ascii(field);
    }
    let mut payload = w.into_bytes();
    let signature = sign_typed(
        seed,
        ADMISSION_TRUST_DOMAIN,
        ADMISSION_PAYLOAD_TYPE,
        &payload,
    );
    payload.extend_from_slice(&signature);
    base64url_encode(&payload)
}

#[cfg(test)]
mod tests {
    use super::super::admission::{generate_keys, issue_admission_credential};
    use super::*;
    pub(super) fn context() -> AllocationContext {
        AllocationContext {
            server_audience: "ds-a".into(),
            game_id: "game-a".into(),
            game_release_id: "release-a".into(),
            contract_id: "lumio.gameplay-envelope.v1".into(),
            room_id: "room-a".into(),
            allocation_id: "allocation-a".into(),
        }
    }
    #[test]
    fn bound_ticket_requires_every_trusted_dimension() {
        let keys = generate_keys();
        let context = context();
        let ticket = issue_bound_test_credential(&keys.seed, &context, 1010);
        let clock = SharedClock::test();
        let verifier = BoundAdmissionVerifier::new(
            context.clone(),
            1,
            keys.public.to_vec(),
            clock.clone(),
            1000,
        )
        .unwrap();
        assert!(verifier.verify(&ticket).is_ok());
        // Reuse within its lease is permitted by the current bearer policy.
        assert!(verifier.verify(&ticket).is_ok());
        for index in 0..6 {
            let mut other = context.clone();
            match index {
                0 => other.server_audience.push('x'),
                1 => other.game_id.push('x'),
                2 => other.game_release_id.push('x'),
                3 => other.contract_id.push('x'),
                4 => other.room_id.push('x'),
                _ => other.allocation_id.push('x'),
            }
            let mismatch =
                BoundAdmissionVerifier::new(other, 1, keys.public.to_vec(), clock.clone(), 1000)
                    .unwrap();
            assert_eq!(
                mismatch.verify(&ticket).err().as_deref(),
                Some("admission_binding_mismatch")
            );
        }
        assert!(clock.advance_test_clock(11_000));
        assert_eq!(
            verifier.verify(&ticket).err().as_deref(),
            Some("admission_credential_expired")
        );
    }
    #[test]
    fn account_auth_and_old_short_tickets_cannot_enter_a_room() {
        let keys = generate_keys();
        let mut unbound = context();
        unbound.server_audience = UNBOUND.into();
        unbound.game_id = UNBOUND.into();
        unbound.game_release_id = UNBOUND.into();
        unbound.contract_id = UNBOUND.into();
        unbound.room_id = UNBOUND.into();
        unbound.allocation_id = UNBOUND.into();
        let verifier = BoundAdmissionVerifier::new(
            context(),
            1,
            keys.public.to_vec(),
            SharedClock::test(),
            1000,
        )
        .unwrap();
        let ticket = issue_bound_test_credential(&keys.seed, &unbound, 1010);
        assert_eq!(
            verifier.verify(&ticket).err().as_deref(),
            Some("admission_credential_unbound")
        );
        let old = issue_admission_credential(&keys.seed, 1, "acct_a", "Player01", false, 1, 2000);
        assert_eq!(
            verifier.verify(&old).err().as_deref(),
            Some("admission_credential_malformed")
        );
        assert!(verifier
            .verify(&"x".repeat(MAX_CREDENTIAL_BYTES + 1))
            .is_err());
    }
}
