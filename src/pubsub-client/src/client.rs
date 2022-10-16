use pubsub_common::{
    ClientId, GetResponse, MessagePayload, PutResponse, Request, SubscribeResponse, Topic,
    UnsubscribeResponse,
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
        url: String,
        topic: Topic,
        message: Option<String>,
    ) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REQ)?;
        socket
            .connect(&url)
            .expect("Service is unavailable: could not connect");

        Ok(Client {
            operation_type,
            id,
            topic,
            message,
            socket,
        })
    }

    pub fn execute(&mut self) {
        let request: Request = match self.operation_type {
            OperationType::Put => Request::Put(MessagePayload {
                topic: self.topic.to_owned(),
                data: self.message.to_owned().unwrap().into_bytes(),
            }),
            OperationType::Get => Request::Get(self.id.to_owned(), self.topic.to_owned()),
            OperationType::Subscribe => {
                Request::Subscribe(self.id.to_owned(), self.topic.to_owned())
            }
            OperationType::Unsubscribe => {
                Request::Unsubscribe(self.id.to_owned(), self.topic.to_owned())
            }
        };

        let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
        self.socket.send(data, 0).expect("Service is unavailable");

        let mut message = zmq::Message::new();
        self.socket
            .recv(&mut message, 0)
            .expect("No response from server");

        match self.operation_type {
            OperationType::Put => {
                self.process_put(serde_json::from_slice::<PutResponse>(&message).unwrap())
            }
            OperationType::Get => {
                self.process_get(serde_json::from_slice::<GetResponse>(&message).unwrap())
            }
            OperationType::Subscribe => self
                .process_subscribe(serde_json::from_slice::<SubscribeResponse>(&message).unwrap()),
            OperationType::Unsubscribe => self.process_unsubscribe(
                serde_json::from_slice::<UnsubscribeResponse>(&message).unwrap(),
            ),
        }
    }

    fn process_put(&self, _: PutResponse) {
        println!("Message published successfully");
    }

    fn process_get(&self, reply: GetResponse) {
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

    fn process_subscribe(&self, reply: SubscribeResponse) {
        match reply {
            SubscribeResponse::Ok => println!("Subscrurltion done successfully"),
            SubscribeResponse::AlreadySubscribed => {
                println!("You are already subscribed to that topic.")
            }
        }
    }

    fn process_unsubscribe(&self, reply: UnsubscribeResponse) {
        match reply {
            UnsubscribeResponse::Ok => println!("Subscrurltion removed with success"),
            UnsubscribeResponse::NotSubscribed => {
                println!("You are not subscribed for that topic. No action was taken.")
            }
        }
    }
}
