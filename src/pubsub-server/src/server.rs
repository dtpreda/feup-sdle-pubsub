use log::info;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SubscribeResponse, SubscriberId, Topic,
    UnsubscribeResponse,
};

pub struct Server {
    socket: zmq::Socket,
    queue: HashMap<SubscriberId, VecDeque<Rc<Message>>>,
    subscriptions: HashMap<Topic, HashSet<SubscriberId>>,
}

impl Server {
    pub fn new(port: u16) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REP)?;
        socket.set_linger(0)?;
        socket.bind(&format!("tcp://*:{}", port))?;
        println!("Service will listen on port {}", port);

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
                Request::Get(s, t) => serde_json::to_vec(&self.get(&s, &t)),
                Request::Subscribe(s, t) => serde_json::to_vec(&self.subscribe(&s, &t)),
                Request::Unsubscribe(s, t) => serde_json::to_vec(&self.unsubscribe(&s, &t)),
            }
            .unwrap();

            self.socket.send(response, 0).expect("send failed");
        }
    }

    fn put(&mut self, message: Message) -> PutResponse {
        let topic = message.topic.clone();

        if let Some(subscriber_set) = self.subscriptions.get(&message.topic) {
            let msg_rc = Rc::new(message);
            for subscriber in subscriber_set {
                self.queue
                    .get_mut(subscriber)
                    .expect("subscriber does not have a message queue")
                    .push_back(msg_rc.clone());
            }
            info!("New message published on {}", topic);
            return PutResponse::Ok;
        }

        info!("No subscribers exist for {}", topic);
        PutResponse::NoSubscribers
    }

    fn get(&mut self, subscriber: &SubscriberId, topic: &Topic) -> GetResponse {
        match self.subscriptions.get(topic) {
            Some(set) if set.contains(subscriber) => {}
            _ => {
                info!("Subscriber {} is not subscribed to {}", subscriber, topic);
                return GetResponse::NotSubscribed;
            }
        }

        match self.queue.get_mut(subscriber) {
            Some(queue) => {
                let index = queue
                    .iter()
                    .enumerate()
                    .find(|(_, msg)| msg.topic == *topic)
                    .map(|(i, _)| i);
                match index {
                    Some(i) => GetResponse::Ok((*queue.remove(i).unwrap()).clone()),
                    None => {
                        info!("Subscriber {} has no messages for {}", subscriber, topic);
                        GetResponse::NoMessageAvailable
                    }
                }
            }
            None => {
                info!("Subscriber {} has no messages for {}", subscriber, topic);
                GetResponse::NoMessageAvailable
            }
        }
    }

    fn subscribe(&mut self, subscriber: &SubscriberId, topic: &Topic) -> SubscribeResponse {
        if !self.queue.contains_key(subscriber) {
            self.queue.insert(subscriber.to_owned(), VecDeque::new());
        }

        let set = self
            .subscriptions
            .entry(topic.clone())
            .or_insert_with(HashSet::new);
        if set.insert(subscriber.clone()) {
            info!("{} subscribed to {}", subscriber, topic);
            SubscribeResponse::Ok
        } else {
            info!("{} already subscribed to {}", subscriber, topic);
            SubscribeResponse::AlreadySubscribed
        }
    }

    fn unsubscribe(&mut self, subscriber: &SubscriberId, topic: &Topic) -> UnsubscribeResponse {
        let topic_subscriptions = match self.subscriptions.get_mut(topic) {
            Some(set) => set,
            None => {
                info!("No subscriptions exist for topic {}", topic);
                return UnsubscribeResponse::NotSubscribed;
            }
        };

        if topic_subscriptions.remove(subscriber) {
            info!(
                "Subscriber with id {} unsubscribed from topic {}",
                subscriber, topic
            );
            UnsubscribeResponse::Ok
        } else {
            info!(
                "Subscriber with id {} was not subscribed to topic {}",
                subscriber, topic
            );
            UnsubscribeResponse::NotSubscribed
        }
    }
}
