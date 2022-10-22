use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
    path::PathBuf,
};

use pubsub_common::{
    ClientId, GetResponse, Message, PutResponse, Request, SequenceNumber, SubscribeResponse, Topic,
    UnsubscribeResponse, MAX_RETRIES, RETRY_DELAY_MS,
};

use super::Operation;

pub fn perform_operation(
    client_id: ClientId,
    service_url: String,
    operation: Operation,
) -> Result<(), zmq::Error> {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SocketType::REQ)?;
    socket.set_linger(0)?;
    socket.set_rcvtimeo(RETRY_DELAY_MS)?;
    socket
        .connect(&service_url)
        .expect("Service is unavailable: could not connect");

    let mut client_put_sequences = read_client_put_sequences_from_disk(&client_id);
    send_request(&client_id, &operation, &socket, &client_put_sequences)?;
    receive_and_handle_response(&operation, &socket, &mut client_put_sequences, &client_id)
}

fn send_request(
    client_id: &ClientId,
    operation: &Operation,
    socket: &zmq::Socket,
    client_put_sequences: &HashMap<Topic, SequenceNumber>,
) -> Result<(), zmq::Error> {
    let request: Request = match operation {
        Operation::Put { topic, message } => Request::Put(
            Message {
                topic: topic.to_string(),
                data: message.clone().into_bytes(),
            },
            client_id.to_string(),
            *client_put_sequences.get(topic).unwrap_or(&0),
        ),
        Operation::Get { topic } => Request::Get(client_id.to_string(), topic.to_string()),
        Operation::Subscribe { topic } => {
            Request::Subscribe(client_id.to_string(), topic.to_string())
        }
        Operation::Unsubscribe { topic } => {
            Request::Unsubscribe(client_id.to_string(), topic.to_string())
        }
    };

    let data: Vec<u8> = serde_json::to_vec(&request).unwrap();
    socket.send(data, 0)
}

fn receive_and_handle_response(
    operation: &Operation,
    socket: &zmq::Socket,
    client_put_sequences: &mut HashMap<Topic, SequenceNumber>,
    client_id: &ClientId,
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
            client_id,
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
    client_id: &ClientId,
) {
    let client_sequence_number = client_put_sequences.get(topic).unwrap_or(&0);
    match reply {
        PutResponse::Ok => {
            client_put_sequences.insert(topic.to_string(), client_sequence_number + 1);
            println!("Message published successfully")
        }
        PutResponse::RepeatedMessage(sequence_number) => {
            client_put_sequences.insert(topic.to_string(), sequence_number);
            eprintln!("Repeated message")
        }
        PutResponse::InvalidSequenceNumber(sequence_number) => {
            client_put_sequences.insert(topic.to_string(), sequence_number);
            eprintln!("Invalid sequence number")
        }
    }

    write_client_put_sequences_to_disk(client_put_sequences, client_id);
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

fn write_client_put_sequences_to_disk(
    client_put_sequences: &HashMap<Topic, SequenceNumber>,
    client_id: &ClientId,
) {
    let mut dir = PathBuf::from("client_data");
    dir.push(client_id);
    fs::create_dir_all(&dir).expect("failed to create data directory");

    let temp_file_name = dir.join("put_sequences.json.new");
    let file_name = dir.join("put_sequences.json");

    let mut file = fs::File::create(&temp_file_name).expect("Could not create file");
    serde_json::to_writer(&mut file, &client_put_sequences).expect("Could not write to file");
    file.sync_all().expect("Could not sync file");
    fs::rename(temp_file_name, file_name).expect("Could not rename file");
}

fn read_client_put_sequences_from_disk(client_id: &ClientId) -> HashMap<Topic, SequenceNumber> {
    match fs::File::open(format!("client_data/{}/put_sequences.json", client_id)) {
        Ok(file) => serde_json::from_reader(file).unwrap(),
        Err(err) if err.kind() == io::ErrorKind::NotFound => HashMap::new(),
        Err(err) => panic!("failed to open sequence numbers file: {}", err),
    }
}
