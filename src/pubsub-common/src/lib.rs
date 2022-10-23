use serde::{Deserialize, Serialize};

pub type Topic = String;
pub type ClientId = String;
pub type SequenceNumber = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub topic: Topic,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SequentialMessage {
    pub message: Message,
    pub sequence_number: SequenceNumber,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Get(ClientId, Topic, SequenceNumber),
    Put(Message, ClientId, SequenceNumber),
    Subscribe(ClientId, Topic),
    Unsubscribe(ClientId, Topic),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PutResponse {
    Ok,
    RepeatedMessage(SequenceNumber),
    InvalidSequenceNumber(SequenceNumber),
    InvalidTopicName(Topic),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(SequentialMessage),
    NoMessageAvailable,
    InvalidSequenceNumber(SequenceNumber),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscribeResponse {
    Ok,
    AlreadySubscribed,
    InvalidTopicName(Topic),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UnsubscribeResponse {
    Ok,
    NotSubscribed,
}

pub const MAX_RETRIES: u8 = 3;
pub const RETRY_DELAY_MS: i32 = 2000;
