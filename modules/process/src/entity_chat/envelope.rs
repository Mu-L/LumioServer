//! Acceptance-client construction for a frozen C-1 chat input.
//!
//! Validation and decoding stay exclusively in Runtime `WireCodec`.

use sha2::{Digest, Sha256};

use super::crypto::{hex_lower, BinWriter};

pub const MESSAGE_TYPE: &str = "InputCommand";
pub const CHAT_INPUT_MAPPING: &str = "chat.input";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandBlock {
    pub mapping_id: String,
    pub payload: String,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputCommand {
    pub message_type: String,
    pub commands: Vec<CommandBlock>,
}

impl InputCommand {
    #[must_use]
    pub fn from_chat_text(text: &str) -> Self {
        let mut writer = BinWriter::new();
        writer.write_ascii(text);
        let payload = writer.into_bytes();
        Self {
            message_type: MESSAGE_TYPE.to_owned(),
            commands: vec![CommandBlock {
                mapping_id: CHAT_INPUT_MAPPING.to_owned(),
                payload: hex_lower(&payload),
                payload_sha256: hex_lower(&Sha256::digest(&payload)),
            }],
        }
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::json!({
            "messageType": self.message_type,
            "commands": self.commands.iter().map(|block| {
                serde_json::json!({
                    "mappingId": block.mapping_id,
                    "payload": block.payload,
                    "payloadSha256": block.payload_sha256,
                })
            }).collect::<Vec<_>>(),
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gg_matches_frozen_hash_example() {
        let envelope = InputCommand::from_chat_text("gg");
        assert_eq!(envelope.message_type, MESSAGE_TYPE);
        assert_eq!(envelope.commands.len(), 1);
        assert_eq!(envelope.commands[0].mapping_id, CHAT_INPUT_MAPPING);
        assert_eq!(envelope.commands[0].payload, "020000006767");
        assert_eq!(
            envelope.commands[0].payload_sha256,
            "5dbd584f1718b8bcd0dab4abeea83169f4a990defab81a8316ed845798d92dab"
        );
    }
}
