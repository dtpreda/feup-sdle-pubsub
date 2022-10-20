use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};

use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SequenceNumber, SubscribeResponse, SubscriberId,
    Topic, UnsubscribeResponse,
};

use super::Operation;

pub fn perform_operation(url: String, operation: Operation) -> Result<(), zmq::Error> {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SocketType::REQ)?;
    socket
        .connect(&url)
        .expect("Service is unavailable: could not connect");

    let client_sequences: HashMap<Topic, HashMap<SubscriberId, SequenceNumber>> =
        match fs::File::open("client_sequences.json") {
            Ok(mut file) => serde_json::from_reader(&mut file).unwrap(),
            Err(_) => HashMap::new(),
        };
    send_request(&operation, &socket, &client_sequences)?;
    receive_and_handle_response(&operation, &socket, client_sequences)
}

fn send_request(
    operation: &Operation,
    socket: &zmq::Socket,
    client_sequences: &HashMap<Topic, HashMap<SubscriberId, SequenceNumber>>,
) -> Result<(), zmq::Error> {
    let request: Request = match operation {
        Operation::Put { topic, message } => Request::Put(Message {
            topic: topic.to_string(),
            data: message.clone().into_bytes(),
        }),
        Operation::Get { id, topic } => Request::Get(
            id.to_string(),
            topic.to_string(),
            *client_sequences
                .get(topic)
                .unwrap_or(&HashMap::new())
                .get(id)
                .unwrap_or(&0),
        ),
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
    client_sequences: HashMap<Topic, HashMap<SubscriberId, SequenceNumber>>,
) -> Result<(), zmq::Error> {
    let mut message = zmq::Message::new();
    socket
        .recv(&mut message, 0)
        .expect("No response from server");

    match operation {
        Operation::Put { .. } => {
            process_put(serde_json::from_slice::<PutResponse>(&message).unwrap())
        }
        Operation::Get { id, topic } => process_get(
            serde_json::from_slice::<GetResponse>(&message).unwrap(),
            client_sequences,
            id,
            topic,
        ),
        Operation::Subscribe { .. } => {
            process_subscribe(serde_json::from_slice::<SubscribeResponse>(&message).unwrap())
        }
        Operation::Unsubscribe { .. } => {
            process_unsubscribe(serde_json::from_slice::<UnsubscribeResponse>(&message).unwrap())
        }
    };

    Ok(())
}

fn process_put(_: PutResponse) {
    println!("Message published successfully");
}

fn process_get(
    reply: GetResponse,
    mut client_sequences: HashMap<Topic, HashMap<SubscriberId, SequenceNumber>>,
    id: &SubscriberId,
    topic: &Topic,
) -> () {
    match reply {
        GetResponse::Ok(seq_message) => {
            client_sequences
                .entry(topic.to_owned())
                .or_insert_with(HashMap::new)
                .insert(id.to_owned(), seq_message.sequence_number);

            let mut file =
                fs::File::create("client_sequences.json.new").expect("Internal client error");
            serde_json::to_writer(&mut file, &client_sequences).expect("Internal client error");
            file.sync_all().expect("Internal client error");
            fs::rename("client_sequences.json.new", "client_sequences.json")
                .expect("Internal client error");

            io::stdout()
                .write_all(&seq_message.message.data)
                .expect("IO error while writing to stdout");
        }
        GetResponse::NotSubscribed => eprintln!("You are not subscribed for that topic"),
        GetResponse::NoMessageAvailable => eprintln!("No message is available from that topic"),
        GetResponse::InvalidSequenceNumber(sn) => {
            eprintln!("Invalid sequence number. Should be: {}", sn)
        }
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
