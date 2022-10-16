use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SubscribeResponse, ClientId, Topic,
    UnsubscribeResponse,
};

pub struct Server {
    socket: zmq::Socket,
    queue: HashMap<ClientId, VecDeque<Rc<Message>>>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
}

impl Server {
    pub fn new(port: u16) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REP)?;
        socket.bind(&format!("tcp://*:{}", port))?;

        Ok(Server {
            socket,
            queue: HashMap::new(),
            subscriptions: HashMap::new(),
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
                Request::Get(s, t) => serde_json::to_vec(&self.get(s, t)),
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

    fn get(&mut self, subscriber: ClientId, topic: Topic) -> GetResponse {
        match self.subscriptions.get(&topic) {
            Some(set) if set.contains(&subscriber) => {}
            _ => return GetResponse::NotSubscribed,
        }

        match self.queue.get_mut(&subscriber) {
            Some(queue) => {
                let index = queue
                    .iter()
                    .enumerate()
                    .find(|(_, msg)| msg.topic == topic)
                    .map(|(i, _)| i);
                match index {
                    Some(i) => GetResponse::Ok((*queue.remove(i).unwrap()).clone()),
                    None => GetResponse::NoMessageAvailable,
                }
            }
            None => GetResponse::NoMessageAvailable,
        }
    }

    fn subscribe(&mut self, subscriber: ClientId, topic: Topic) -> SubscribeResponse {
        if !self.queue.contains_key(&subscriber) {
            self.queue.insert(subscriber.to_owned(), VecDeque::new());
        }

        let set = self.subscriptions.entry(topic).or_insert_with(HashSet::new);
        if set.insert(subscriber) {
            SubscribeResponse::Ok
        } else {
            SubscribeResponse::AlreadySubscribed
        }
    }

    fn unsubscribe(&mut self, subscriber: ClientId, topic: Topic) -> UnsubscribeResponse {
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
