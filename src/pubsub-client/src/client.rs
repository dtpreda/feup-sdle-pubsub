use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SubscribeResponse, SubscriberId, Topic,
    UnsubscribeResponse,
};

#[derive(Copy, Clone)]
pub enum OperationType {
    Put,
    Get,
    Subscribe,
    Unsubscribe,
}

pub fn service_execute(
    operation_type: OperationType,
    id: SubscriberId,
    url: String,
    topic: Topic,
    message: Option<String>,
) -> Result<(), zmq::Error> {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SocketType::REQ)?;
    socket
        .connect(&url)
        .expect("Service is unavailable: could not connect");

    send_action(operation_type, id, topic, message, &socket)?;

    receive_action_response(operation_type, &socket)?;

    Ok(())
}

fn send_action(
    operation_type: OperationType,
    id: SubscriberId,
    topic: Topic,
    message: Option<String>,
    socket: &zmq::Socket,
) -> Result<(), zmq::Error> {
    let request: Request = match operation_type {
        OperationType::Put => Request::Put(Message {
            topic: topic,
            data: message.unwrap().into_bytes(),
        }),
        OperationType::Get => Request::Get(id, topic),
        OperationType::Subscribe => Request::Subscribe(id, topic),
        OperationType::Unsubscribe => Request::Unsubscribe(id, topic),
    };

    let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
    socket.send(data, 0).expect("Service is unavailable");

    Ok(())
}

fn receive_action_response(
    operation_type: OperationType,
    socket: &zmq::Socket,
) -> Result<(), zmq::Error> {
    let mut message = zmq::Message::new();
    socket
        .recv(&mut message, 0)
        .expect("No response from server");

    match operation_type {
        OperationType::Put => process_put(serde_json::from_slice::<PutResponse>(&message).unwrap()),
        OperationType::Get => process_get(serde_json::from_slice::<GetResponse>(&message).unwrap()),
        OperationType::Subscribe => {
            process_subscribe(serde_json::from_slice::<SubscribeResponse>(&message).unwrap())
        }
        OperationType::Unsubscribe => {
            process_unsubscribe(serde_json::from_slice::<UnsubscribeResponse>(&message).unwrap())
        }
    }

    Ok(())
}

fn process_put(_: PutResponse) {
    println!("Message published successfully");
}

fn process_get(reply: GetResponse) {
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

fn process_subscribe(reply: SubscribeResponse) {
    match reply {
        SubscribeResponse::Ok => println!("Subscription done successfully"),
        SubscribeResponse::AlreadySubscribed => {
            println!("You are already subscribed to that topic.")
        }
    }
}

fn process_unsubscribe(reply: UnsubscribeResponse) {
    match reply {
        UnsubscribeResponse::Ok => println!("Subscription removed with success"),
        UnsubscribeResponse::NotSubscribed => {
            println!("You are not subscribed for that topic. No action was taken.")
        }
    }
}
