use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use tracing::{debug, info, span, trace, Level};

use pubsub_common::{
    GetResponse, Message, PublisherId, PutResponse, Request, SequenceNumber, SubscribeResponse,
    SubscriberId, Topic, UnsubscribeResponse,
};

pub struct Server {
    socket: zmq::Socket,
    queue: HashMap<SubscriberId, VecDeque<Rc<Message>>>,
    subscriptions: HashMap<Topic, HashSet<SubscriberId>>,
    client_put_sequences: HashMap<Topic, HashMap<PublisherId, SequenceNumber>>,
}

impl Server {
    pub fn new(port: u16) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REP)?;
        socket.bind(&format!("tcp://*:{}", port))?;
        info!("listening on port {}", port);

        Ok(Server {
            socket,
            queue: HashMap::new(),
            subscriptions: HashMap::new(),
            client_put_sequences: HashMap::new(),
        })
    }

    pub fn run(&mut self) {
        let mut message = zmq::Message::new();
        loop {
            let span = span!(Level::DEBUG, "request");
            let _enter = span.enter();

            self.socket.recv(&mut message, 0).expect("recv failed");
            debug!("received request");

            let request: Request =
                serde_json::from_slice(&message).expect("request parsing failed");
            trace!(?request);

            let response = match request {
                Request::Put(m, i, s) => serde_json::to_vec(&self.put(m, i, s)),
                Request::Get(s, t) => serde_json::to_vec(&self.get(s, t)),
                Request::Subscribe(s, t) => serde_json::to_vec(&self.subscribe(s, t)),
                Request::Unsubscribe(s, t) => serde_json::to_vec(&self.unsubscribe(s, t)),
            }
            .unwrap();

            debug!("sending response");
            self.socket.send(response, 0).expect("send failed");
        }
    }

    fn put(
        &mut self,
        message: Message,
        publisher_id: PublisherId,
        client_sequence_number: SequenceNumber,
    ) -> PutResponse {
        let span = span!(Level::DEBUG, "put");
        let _enter = span.enter();

        let server_side_sequence_number = self
            .client_put_sequences
            .entry(message.topic.clone())
            .or_default()
            .entry(publisher_id)
            .or_insert(0);
        trace!(client_sequence_number, server_side_sequence_number);

        if *server_side_sequence_number > client_sequence_number {
            debug!(
                seq = client_sequence_number,
                "client sent an already received message"
            );
            return PutResponse::RepeatedMessage;
        } else if *server_side_sequence_number < client_sequence_number {
            debug!(
                expected = server_side_sequence_number,
                received = client_sequence_number,
                "received message is a few messages ahead of what was expected"
            );
            return PutResponse::InvalidSequenceNumber;
        }

        *server_side_sequence_number += 1;

        match self.subscriptions.get(&message.topic) {
            Some(subscriber_set) => {
                let msg_rc = Rc::new(message);
                debug!(message = ?msg_rc, "storing message");

                for subscriber in subscriber_set {
                    trace!(subscriber, "adding message to queue");
                    self.queue
                        .get_mut(subscriber)
                        .expect("subscriber does not have a message queue")
                        .push_back(msg_rc.clone());
                }
            }
            None => debug!(
                topic = message.topic,
                "topic has no subscribers, dropping message"
            ),
        }

        PutResponse::Ok
    }

    fn get(&mut self, subscriber: SubscriberId, topic: Topic) -> GetResponse {
        let span = span!(Level::DEBUG, "get");
        let _enter = span.enter();

        match self.subscriptions.get(&topic) {
            Some(set) if set.contains(&subscriber) => {}
            _ => {
                debug!(subscriber, topic, "not subscribed to topic");
                return GetResponse::NotSubscribed;
            }
        }

        match self.queue.get_mut(&subscriber) {
            Some(queue) => {
                let index = queue
                    .iter()
                    .enumerate()
                    .find(|(_, msg)| msg.topic == topic)
                    .map(|(i, _)| i);
                match index {
                    Some(i) => {
                        let message = queue.remove(i).unwrap();
                        debug!(
                            subscriber,
                            topic,
                            ?message,
                            "removed message from queue to send to the client"
                        );
                        GetResponse::Ok((*message).clone())
                    }
                    None => {
                        debug!(subscriber, topic, "no messages available");
                        GetResponse::NoMessageAvailable
                    }
                }
            }
            None => {
                debug!(subscriber, topic, "no messages available");
                GetResponse::NoMessageAvailable
            }
        }
    }

    fn subscribe(&mut self, subscriber: SubscriberId, topic: Topic) -> SubscribeResponse {
        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();
        debug!(subscriber, topic, "subscribing to topic");

        if !self.queue.contains_key(&subscriber) {
            self.queue.insert(subscriber.to_owned(), VecDeque::new());
        }

        let set = self.subscriptions.entry(topic).or_insert_with(HashSet::new);
        if set.insert(subscriber) {
            SubscribeResponse::Ok
        } else {
            debug!("topic was already subscribed");
            SubscribeResponse::AlreadySubscribed
        }
    }

    fn unsubscribe(&mut self, subscriber: SubscriberId, topic: Topic) -> UnsubscribeResponse {
        let span = span!(Level::DEBUG, "unsubscribe");
        let _enter = span.enter();

        let set = match self.subscriptions.get_mut(&topic) {
            Some(set) => set,
            None => return UnsubscribeResponse::NotSubscribed,
        };

        if set.remove(&subscriber) {
            debug!(subscriber, topic, "unsubscribed from topic");
            UnsubscribeResponse::Ok
        } else {
            debug!(
                subscriber,
                topic, "topic was not subscribed, cannot unsubscribe"
            );
            UnsubscribeResponse::NotSubscribed
        }
    }
}
