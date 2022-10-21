use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use tracing::{debug, info, span, trace, Level};

use pubsub_common::{
    ClientId, GetResponse, Message, PutResponse, Request, SequenceNumber, SubscribeResponse, Topic,
    UnsubscribeResponse,
};

pub struct Server {
    socket: zmq::Socket,
    queue: HashMap<(ClientId, Topic), VecDeque<Rc<Vec<u8>>>>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    client_put_sequences: HashMap<Topic, HashMap<ClientId, SequenceNumber>>,
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
        publisher_id: ClientId,
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

        match client_sequence_number.cmp(server_side_sequence_number) {
            Ordering::Greater => {
                debug!(
                    expected = server_side_sequence_number,
                    received = client_sequence_number,
                    "received message is a few messages ahead of what was expected"
                );
                return PutResponse::InvalidSequenceNumber(*server_side_sequence_number);
            }
            Ordering::Less => {
                debug!(
                    seq = client_sequence_number,
                    "client sent an already received message"
                );
                return PutResponse::RepeatedMessage(*server_side_sequence_number);
            }
            Ordering::Equal => *server_side_sequence_number += 1,
        }

        match self.subscriptions.get(&message.topic) {
            Some(subscriber_set) => {
                let data_rc = Rc::new(message.data);
                debug!(message = ?data_rc, "storing message");

                for subscriber in subscriber_set {
                    trace!(subscriber, "adding message to queue");
                    self.queue
                        .get_mut(&(subscriber.to_owned(), message.topic.to_owned()))
                        .expect("subscriber does not have a message queue")
                        .push_back(data_rc.clone());
                }
            }
            None => debug!(
                topic = message.topic,
                "topic has no subscribers, dropping message"
            ),
        }

        PutResponse::Ok
    }

    fn get(&mut self, subscriber: ClientId, topic: Topic) -> GetResponse {
        let span = span!(Level::DEBUG, "get");
        let _enter = span.enter();

        match self
            .queue
            .get_mut(&(subscriber.to_owned(), topic.to_owned()))
        {
            Some(queue) => match queue.pop_front() {
                Some(data) => {
                    debug!(
                        subscriber,
                        topic,
                        ?data,
                        "removed message from queue to send to the client"
                    );
                    GetResponse::Ok(Message {
                        topic,
                        data: data.to_vec(),
                    })
                }
                None => {
                    debug!(subscriber, topic, "no messages available");
                    GetResponse::NoMessageAvailable
                }
            },
            None => {
                debug!(subscriber, topic, "no messages available");
                GetResponse::NoMessageAvailable
            }
        }
    }

    fn subscribe(&mut self, subscriber: ClientId, topic: Topic) -> SubscribeResponse {
        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();
        debug!(subscriber, topic, "subscribing to topic");

        if !self
            .queue
            .contains_key(&(subscriber.to_owned(), topic.to_owned()))
        {
            self.queue
                .insert((subscriber.to_owned(), topic.to_owned()), VecDeque::new());
        }

        let set = self.subscriptions.entry(topic).or_insert_with(HashSet::new);
        if set.insert(subscriber) {
            SubscribeResponse::Ok
        } else {
            debug!("topic was already subscribed");
            SubscribeResponse::AlreadySubscribed
        }
    }

    fn unsubscribe(&mut self, subscriber: ClientId, topic: Topic) -> UnsubscribeResponse {
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
