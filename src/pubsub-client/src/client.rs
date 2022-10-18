use std::io::{self, Write};

use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SubscribeResponse, UnsubscribeResponse,
};

use super::Operation;

pub fn perform_operation(url: String, operation: Operation) -> Result<(), zmq::Error> {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SocketType::REQ)?;
    socket
        .connect(&url)
        .expect("Service is unavailable: could not connect");

    send_request(&operation, &socket)?;
    receive_and_handle_response(&operation, &socket)
}

fn send_request(operation: &Operation, socket: &zmq::Socket) -> Result<(), zmq::Error> {
    let request: Request = match operation {
        Operation::Put { topic, message } => Request::Put(Message {
            topic: topic.to_string(),
            data: message.clone().into_bytes(),
        }),
        Operation::Get { id, topic } => Request::Get(id.to_string(), topic.to_string()),
        Operation::Subscribe { id, topic } => Request::Subscribe(id.to_string(), topic.to_string()),
        Operation::Unsubscribe { id, topic } => {
            Request::Unsubscribe(id.to_string(), topic.to_string())
        }
    };

    let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
    socket.send(data, 0)
}

fn receive_and_handle_response(
    operation: &Operation,
    socket: &zmq::Socket,
) -> Result<(), zmq::Error> {
    let mut message = zmq::Message::new();
    socket
        .recv(&mut message, 0)
        .expect("No response from server");

    match operation {
        Operation::Put { .. } => {
            process_put(serde_json::from_slice::<PutResponse>(&message).unwrap())
        }
        Operation::Get { .. } => {
            process_get(serde_json::from_slice::<GetResponse>(&message).unwrap())
        }
        Operation::Subscribe { .. } => {
            process_subscribe(serde_json::from_slice::<SubscribeResponse>(&message).unwrap())
        }
        Operation::Unsubscribe { .. } => {
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
        GetResponse::Ok(message) => io::stdout()
            .write_all(&message.data)
            .expect("IO error while writing to stdout"),
        GetResponse::NotSubscribed => eprintln!("You are not subscribed for that topic"),
        GetResponse::NoMessageAvailable => eprintln!("No message is available from that topic"),
    }
}

fn process_subscribe(reply: SubscribeResponse) {
    match reply {
        SubscribeResponse::Ok => println!("Subscription done successfully"),
        SubscribeResponse::AlreadySubscribed => {
            eprintln!("You are already subscribed to that topic.")
        }
    }
}

fn process_unsubscribe(reply: UnsubscribeResponse) {
    match reply {
        UnsubscribeResponse::Ok => println!("Subscription removed with success"),
        UnsubscribeResponse::NotSubscribed => {
            eprintln!("You are not subscribed for that topic. No action was taken.")
        }
    }
}
