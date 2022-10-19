use std::io::{self, Write};
use std::collections::HashMap;
use std::fs;
use serde_json;

use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SubscribeResponse, UnsubscribeResponse, SubscriberId, SequenceNumber, Topic
};

use super::Operation;

pub fn perform_operation(url: String, operation: Operation) -> Result<(), zmq::Error> {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SocketType::REQ)?;
    socket
        .connect(&url)
        .expect("Service is unavailable: could not connect");

    let mut client_sequences_file = match fs::File::open("./client_sequences.json") {
        Ok(file) => file,
        Err(_) => fs::File::create("./client_sequences.json").unwrap(),
    };
    let client_sequences : HashMap<Topic, HashMap<SubscriberId, SequenceNumber>> = match serde_json::from_reader(&mut client_sequences_file) {
        Ok(client_sequences) => client_sequences,
        Err(_) => {
            println!("Could not read client_sequences.json");    
            HashMap::new()
        },
    };
    send_request(&operation, &socket, &client_sequences)?;
    receive_and_handle_response(&operation, &socket, client_sequences)
}

fn send_request(operation: &Operation, socket: &zmq::Socket, client_sequences: &HashMap<Topic, HashMap<SubscriberId, SequenceNumber>>) -> Result<(), zmq::Error> {
    let request: Request = match operation {
        Operation::Put { topic, message } => Request::Put(Message {
            topic: topic.to_string(),
            data: message.clone().into_bytes(),
        }),
        Operation::Get { id, topic } => {
            match client_sequences.get(&topic.to_string()) {
                Some(map) if map.contains_key(&id.to_string()) => {
                    Request::Get(id.to_string(), topic.to_string(), map.get(&id.to_string()).unwrap().clone())
                },
                _ => {
                    Request::Get(id.to_string(), topic.to_string(), 0)
                }
            }
        },
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
    client_sequences: HashMap<Topic, HashMap<SubscriberId, SequenceNumber>>
) -> Result<(), zmq::Error> {
    let mut message = zmq::Message::new();
    socket
        .recv(&mut message, 0)
        .expect("No response from server");

    match operation {
        Operation::Put { .. } => {
            process_put(serde_json::from_slice::<PutResponse>(&message).unwrap())
        }
        Operation::Get { id, topic } => {
            process_get(serde_json::from_slice::<GetResponse>(&message).unwrap(), client_sequences, id, topic)
        }
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

fn process_get(reply: GetResponse, mut client_sequences: HashMap<Topic, HashMap<SubscriberId, SequenceNumber>>, id: &SubscriberId, topic: &Topic) -> () {
    match reply {
        GetResponse::Ok(seq_message) => {
            match client_sequences.get(&topic.to_string()) {
                Some(map) if map.contains_key(&id.to_string()) => {
                    client_sequences.get_mut(&topic.to_string()).unwrap().insert(id.to_string(), seq_message.sequence_number);
                },
                _ => {
                    let mut map = HashMap::new();
                    map.insert(id.to_string(), seq_message.sequence_number);
                    client_sequences.insert(topic.to_string(), map);
                }
            }
            let mut file = match fs::File::create("client_sequences.json.new") {
                Ok(file) => file,
                Err(_) => panic!("Internal client error"),
            };
            match serde_json::to_writer(&mut file, &client_sequences) {
                Ok(_) => (),
                Err(_) => panic!("Internal client error"),
            };
            match file.sync_all() {
                Ok(_) => (),
                Err(_) => panic!("Internal client error"),
            };
            match fs::rename("client_sequences.json.new", "client_sequences.json") {
                Ok(_) => (),
                Err(_) => panic!("Internal client error"),
            };
            io::stdout()
            .write_all(&seq_message.message.data)
            .expect("IO error while writing to stdout");
        },
        GetResponse::NotSubscribed => eprintln!("You are not subscribed for that topic"),
        GetResponse::NoMessageAvailable => eprintln!("No message is available from that topic"),
        GetResponse::InvalidSequenceNumber(sn) => eprintln!("Invalid sequence number. Should be: {}", sn)
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
