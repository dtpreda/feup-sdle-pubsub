use pubsub_common::{ClientId, Message, Topic};

pub enum OperationType {
    Put,
    Get,
    Subscribe,
    Unsubscribe,
}

pub struct Client {
    operation_type: OperationType,
    id: ClientId,
    port: u16,
    topic: Topic,
    message: Option<String>,
    socket: zmq::Socket,
}

impl Client {
    pub fn new(
        operation_type: OperationType,
        id: ClientId,
        port: u16,
        topic: Topic,
        message: Option<String>,
    ) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REQ)?;
        socket.connect(&format!("tcp://*:{}", port))?;

        Ok(Client {
            operation_type,
            id,
            port,
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
        self.socket.send(data, 0);

        // Receive here...
    }

    pub fn get(&mut self) {
        let request = pubsub_common::Request::Get(self.id.clone(), self.topic.clone());
        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0);

        // Receive here...
    }

    pub fn subscribe(&mut self) {
        let request = pubsub_common::Request::Subscribe(self.id.clone(), self.topic.clone());
        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0);

        // Receive here...
    }

    pub fn unsubscribe(&mut self) {
        let request = pubsub_common::Request::Unsubscribe(self.id.clone(), self.topic.clone());
        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0);

        // Receive here...
    }
}
