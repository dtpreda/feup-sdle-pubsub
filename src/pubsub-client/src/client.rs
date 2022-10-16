use pubsub_common::{
    ClientId, GetResponse, Message, PutResponse, SubscribeResponse, Topic, UnsubscribeResponse,
};

pub enum OperationType {
    Put,
    Get,
    Subscribe,
    Unsubscribe,
}

pub struct Client {
    operation_type: OperationType,
    id: ClientId,
    topic: Topic,
    message: Option<String>,
    socket: zmq::Socket,
}

impl Client {
    pub fn new(
        operation_type: OperationType,
        id: ClientId,
        ip: Option<String>,
        topic: Topic,
        message: Option<String>,
    ) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REQ)?;
        socket
            .connect(&format!(
                "tcp://{}",
                ip.unwrap_or("localhost:5555".to_string())
            ))
            .expect("Service is unavailable");

        Ok(Client {
            operation_type,
            id,
            topic,
            message,
            socket,
        })
    }

    pub fn execute(&mut self) {
        match self.operation_type {
            OperationType::Put => self.put(pubsub_common::Message {
                topic: self.topic.clone(),
                data: self.message.clone().unwrap().into_bytes(),
            }),
            OperationType::Get => self.get(),
            OperationType::Subscribe => self.subscribe(),
            OperationType::Unsubscribe => self.unsubscribe(),
        }
    }

    pub fn put(&mut self, message: Message) {
        let request = pubsub_common::Request::Put(message);
        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0).expect("Service is unavailable");

        let mut message = zmq::Message::new();
        if let Err(_) = self.socket.recv(&mut message, 0) {
            println!("Error publishing message");
            return;
        }

        serde_json::from_slice::<PutResponse>(&message).expect("request parsing failed");
        println!("Message published successfully");
    }

    pub fn get(&mut self) {
        let request = pubsub_common::Request::Get(self.id.clone(), self.topic.clone());
        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0).expect("Service is unavailable");

        let mut message = zmq::Message::new();
        if let Err(_) = self.socket.recv(&mut message, 0) {
            println!("Error getting message");
            return;
        }

        let reply: GetResponse = serde_json::from_slice(&message).expect("request parsing failed");
        match reply {
            GetResponse::Ok(message) => {
                println!(
                    "Received message: '{}'",
                    std::str::from_utf8(&message.data).unwrap()
                )
            }
            GetResponse::NotSubscribed => println!("You are not subscribed for that topic"),
            GetResponse::NoMessageAvailable => println!("No message is available from that topic"),
        }
    }

    pub fn subscribe(&mut self) {
        let request = pubsub_common::Request::Subscribe(self.id.clone(), self.topic.clone());
        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0).expect("Service is unavailable");

        let mut message = zmq::Message::new();
        if let Err(_) = self.socket.recv(&mut message, 0) {
            println!("Error subscribing to topic");
            return;
        }

        let reply: SubscribeResponse =
            serde_json::from_slice(&message).expect("request parsing failed");
        match reply {
            SubscribeResponse::Ok => println!("Subscription done successfully"),
            SubscribeResponse::AlreadySubscribed => {
                println!("You are already subscribed to that topic.")
            }
        }
    }

    pub fn unsubscribe(&mut self) {
        let request = pubsub_common::Request::Unsubscribe(self.id.clone(), self.topic.clone());
        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0).expect("Service is unavailable");

        let mut message = zmq::Message::new();
        if let Err(_) = self.socket.recv(&mut message, 0) {
            println!("Error subscribing to topic");
            return;
        }

        let reply: UnsubscribeResponse =
            serde_json::from_slice(&message).expect("request parsing failed");
        match reply {
            UnsubscribeResponse::Ok => println!("Subscription removed with success"),
            UnsubscribeResponse::NotSubscribed => {
                println!("You are not subscribed for that topic. No action was taken.")
            }
        }
    }
}
