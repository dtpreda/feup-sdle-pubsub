use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
};

use pubsub_common::{
    GetResponse, Message, PutResponse, Request, SequenceNumber, SubscribeResponse, Topic,
    UnsubscribeResponse, MAX_RETRIES, RETRY_DELAY_MS,
};

use super::Operation;

pub fn perform_operation(url: String, operation: Operation) -> Result<(), zmq::Error> {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SocketType::REQ)?;
    socket.set_linger(0)?;
    socket.set_rcvtimeo(RETRY_DELAY_MS)?;
    socket
        .connect(&url)
        .expect("Service is unavailable: could not connect");

    // TODO: address the clash of two publishers with different ids on the same machine
    let mut client_put_sequences: HashMap<Topic, SequenceNumber> =
        match fs::File::open("client_put_sequences.json") {
            Ok(file) => serde_json::from_reader(file).unwrap(),
            Err(_) => HashMap::new(),
        };

    send_request(&operation, &socket, &client_put_sequences)?;
    receive_and_handle_response(&operation, &socket, &mut client_put_sequences)
}

fn send_request(
    operation: &Operation,
    socket: &zmq::Socket,
    client_put_sequences: &HashMap<Topic, SequenceNumber>,
) -> Result<(), zmq::Error> {
    let request: Request = match operation {
        Operation::Put { id, topic, message } => Request::Put(
            Message {
                topic: topic.to_string(),
                data: message.clone().into_bytes(),
            },
            id.to_string(),
            *client_put_sequences.get(topic).unwrap_or(&0),
        ),
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
    client_put_sequences: &mut HashMap<Topic, SequenceNumber>,
) -> Result<(), zmq::Error> {
    let mut message = zmq::Message::new();
    for i in 0.. {
        if socket.recv(&mut message, 0).is_ok() {
            break;
        }
        if i == MAX_RETRIES {
            eprintln!("Service is unavailable: no response");
            return Err(zmq::Error::EAGAIN);
        }
    }

    match operation {
        Operation::Put { topic, .. } => process_put(
            serde_json::from_slice::<PutResponse>(&message).unwrap(),
            topic,
            client_put_sequences,
        ),
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

fn process_put(
    reply: PutResponse,
    topic: &Topic,
    client_put_sequences: &mut HashMap<Topic, SequenceNumber>,
) {
    let client_sequence_number = client_put_sequences.get(topic).unwrap_or(&0);
    match reply {
        PutResponse::Ok => {
            client_put_sequences.insert(topic.to_string(), client_sequence_number + 1);
            println!("Message published successfully")
        }
        PutResponse::RepeatedMessage(sequence_number) => {
            client_put_sequences.insert(topic.to_string(), sequence_number);
            println!("Repeated message")
        }
        PutResponse::InvalidSequenceNumber(sequence_number) => {
            client_put_sequences.insert(topic.to_string(), sequence_number);
            println!("Invalid sequence number")
        }
    }

    let mut file =
        fs::File::create("client_put_sequences.json.new").expect("Internal client error");
    serde_json::to_writer(&mut file, &client_put_sequences).expect("Internal client error");
    file.sync_all().expect("Internal client error");
    fs::rename("client_put_sequences.json.new", "client_put_sequences.json")
        .expect("Internal client error");
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
