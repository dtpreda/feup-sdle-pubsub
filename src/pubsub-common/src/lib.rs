use serde::{Deserialize, Serialize};

pub type Topic = String;
pub type SubscriberId = String;
pub type PublisherId = String;
pub type SequenceNumber = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub topic: Topic,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Put(Message, PublisherId, SequenceNumber),
    Get(SubscriberId, Topic),
    Subscribe(SubscriberId, Topic),
    Unsubscribe(SubscriberId, Topic),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PutResponse {
    Ok,
    RepeatedMessage,
    InvalidSequenceNumber,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(Message),
    NotSubscribed,
    NoMessageAvailable,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum SubscribeResponse {
    Ok,
    AlreadySubscribed,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum UnsubscribeResponse {
    Ok,
    NotSubscribed,
}

pub const MAX_RETRIES: u8 = 3;
pub const RETRY_DELAY_MS: i32 = 2000;
