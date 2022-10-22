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

    send_request(&operation, &socket)?;
    receive_and_handle_response(&operation, &socket)
}

fn get_client_directory(id: &SubscriberId) -> String {
    format!("client_data/{}", id)
}

fn get_client_file_name(id: &SubscriberId) -> String {
    format!("client_data/{}/get_sequences.json", id)
}

fn read_client_get_sequence_numbers(id: &SubscriberId) -> HashMap<Topic, SequenceNumber> {
    let client_file_name: String = get_client_file_name(&id);
    match fs::File::open(client_file_name) {
        Ok(mut file) => serde_json::from_reader(&mut file).unwrap(),
        Err(_) => HashMap::new(),
    }
}

fn send_request(operation: &Operation, socket: &zmq::Socket) -> Result<(), zmq::Error> {
    let request: Request = match operation {
        Operation::Put { topic, message } => Request::Put(Message {
            topic: topic.to_string(),
            data: message.clone().into_bytes(),
        }),
        Operation::Get { id, topic } => {
            let client_get_sequences: HashMap<Topic, SequenceNumber> =
                read_client_get_sequence_numbers(&id);
            Request::Get(
                id.to_string(),
                topic.to_string(),
                *client_get_sequences.get(topic).unwrap_or(&0),
            )
        }
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
        Operation::Get { id, topic } => process_get(
            serde_json::from_slice::<GetResponse>(&message).unwrap(),
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

fn write_client_get_sequence_numbers(
    client_get_sequence_numbers: HashMap<Topic, SequenceNumber>,
    id: &SubscriberId,
) {
    fs::create_dir_all(get_client_directory(&id)).unwrap();
    let client_file_name: String = get_client_file_name(&id);
    let mut file = fs::File::create(&client_file_name).unwrap();
    file.write_all(
        serde_json::to_string(&client_get_sequence_numbers)
            .unwrap()
            .as_bytes(),
    )
    .unwrap();

    let mut file =
        fs::File::create("client_data/client_sequences.json.new").expect("Internal client error");
    serde_json::to_writer(&mut file, &client_get_sequence_numbers).expect("Internal client error");
    file.sync_all().expect("Internal client error");
    fs::rename("client_data/client_sequences.json.new", client_file_name)
        .expect("Internal client error");
}

fn process_get(reply: GetResponse, id: &SubscriberId, topic: &Topic) -> () {
    match reply {
        GetResponse::Ok(seq_message) => {
            let mut client_get_sequence_numbers = read_client_get_sequence_numbers(&id);
            client_get_sequence_numbers.insert(topic.to_owned(), seq_message.sequence_number);

            write_client_get_sequence_numbers(client_get_sequence_numbers, id);

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
