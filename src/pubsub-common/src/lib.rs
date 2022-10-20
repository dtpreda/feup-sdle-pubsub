use serde::{Deserialize, Serialize};

pub type Topic = String;
pub type SubscriberId = String;
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
    Put(Message),
    Get(SubscriberId, Topic, SequenceNumber),
    Subscribe(SubscriberId, Topic),
    Unsubscribe(SubscriberId, Topic),
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct PutResponse;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(SequentialMessage),
    NotSubscribed,
    NoMessageAvailable,
    InvalidSequenceNumber(SequenceNumber),
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
