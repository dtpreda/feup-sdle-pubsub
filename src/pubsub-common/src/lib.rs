use serde::{Deserialize, Serialize};

pub type Topic = String;
pub type ClientId = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagePayload {
    pub topic: Topic,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Put(MessagePayload),
    Get(ClientId, Topic),
    Subscribe(ClientId, Topic),
    Unsubscribe(ClientId, Topic),
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct PutResponse;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(MessagePayload),
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
