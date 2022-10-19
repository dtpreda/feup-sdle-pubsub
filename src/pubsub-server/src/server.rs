use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SubscribeResponse, SubscriberId, Topic,
    UnsubscribeResponse, SequentialMessage, SequenceNumber
};

pub struct Server {
    socket: zmq::Socket,
    queue: HashMap<SubscriberId, VecDeque<Rc<Message>>>,
    subscriptions: HashMap<Topic, HashSet<SubscriberId>>,
    client_sequences: HashMap<Topic, HashMap<SubscriberId, SequenceNumber>>,
}

impl Server {
    pub fn new(port: u16) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REP)?;
        socket.bind(&format!("tcp://*:{}", port))?;
        println!("Service will listen on port {}", port);

        Ok(Server {
            socket,
            queue: HashMap::new(),
            subscriptions: HashMap::new(),
            client_sequences: HashMap::new(),
        })
    }

    pub fn run(&mut self) {
        let mut message = zmq::Message::new();
        loop {
            self.socket.recv(&mut message, 0).expect("recv failed");

            let request: Request =
                serde_json::from_slice(&message).expect("request parsing failed");
            let response = match request {
                Request::Put(m) => serde_json::to_vec(&self.put(m)),
                Request::Get(s, t, sn) => serde_json::to_vec(&self.get(s, t, sn)),
                Request::Subscribe(s, t) => serde_json::to_vec(&self.subscribe(s, t)),
                Request::Unsubscribe(s, t) => serde_json::to_vec(&self.unsubscribe(s, t)),
            }
            .unwrap();

            self.socket.send(response, 0).expect("send failed");
        }
    }

    fn put(&mut self, message: Message) -> PutResponse {
        if let Some(subscriber_set) = self.subscriptions.get(&message.topic) {
            let msg_rc = Rc::new(message);
            for subscriber in subscriber_set {
                self.queue
                    .get_mut(subscriber)
                    .expect("subscriber does not have a message queue")
                    .push_back(msg_rc.clone());
            }
        }

        PutResponse {}
    }

    fn get(&mut self, subscriber: SubscriberId, topic: Topic, sequence_number: SequenceNumber) -> GetResponse {
        match self.subscriptions.get(&topic) {
            Some(set) if set.contains(&subscriber) => {}
            _ => return GetResponse::NotSubscribed,
        }

        match self.client_sequences.get(&topic) {
            Some(map) if map.contains_key(&subscriber) => {
                let client_sequence = map.get(&subscriber).unwrap();
                match self.queue.get_mut(&subscriber) {
                    Some(queue) => {
                        if sequence_number == *client_sequence && *client_sequence != 0{
                            let index = queue
                            .iter()
                            .enumerate()
                            .find(|(_, msg)| msg.topic == topic)
                            .map(|(i, _)| i);
                            match index {
                                Some(i) => {
                                    queue.remove(i);
                                }
                                None => { return GetResponse::NoMessageAvailable; }
                            };
                        } else if *client_sequence != 0 && sequence_number != *client_sequence - 1{
                            return GetResponse::InvalidSequenceNumber(*client_sequence);
                        }

                        let client_sequence = self.client_sequences.get_mut(&topic).unwrap();
                        client_sequence.insert(subscriber.clone(), sequence_number + 1);
                        
                        let index = queue
                            .iter()
                            .enumerate()
                            .find(|(_, msg)| msg.topic == topic)
                            .map(|(i, _)| i);
                        match index {
                            Some(i) => {
                                GetResponse::Ok(SequentialMessage {message: queue.get(i).unwrap().as_ref().clone(), sequence_number: sequence_number + 1})
                            }
                            None => GetResponse::NoMessageAvailable,
                        }
                    }
                    None => GetResponse::NoMessageAvailable,
                }
            }
            _ => return GetResponse::NotSubscribed,
        }
    }

    fn subscribe(&mut self, subscriber: SubscriberId, topic: Topic) -> SubscribeResponse {
        if !self.queue.contains_key(&subscriber) {
            self.queue.insert(subscriber.to_owned(), VecDeque::new());
        }
        
        let set = self.subscriptions.entry(topic.to_owned()).or_insert_with(HashSet::new);
        let sequences_set = self.client_sequences.entry(topic).or_insert_with(HashMap::new);
        if set.insert(subscriber.to_owned()) && sequences_set.insert(subscriber, 0).is_none() {
            SubscribeResponse::Ok
        } else {
            SubscribeResponse::AlreadySubscribed
        }
    }

    fn unsubscribe(&mut self, subscriber: SubscriberId, topic: Topic) -> UnsubscribeResponse {
        let set = match self.subscriptions.get_mut(&topic) {
            Some(set) => set,
            None => return UnsubscribeResponse::NotSubscribed,
        };

        if set.remove(&subscriber) {
            UnsubscribeResponse::Ok
        } else {
            UnsubscribeResponse::NotSubscribed
        }
    }
}
