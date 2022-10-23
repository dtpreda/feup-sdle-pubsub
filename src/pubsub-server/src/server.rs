use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use std::rc::Rc;
use std::{fs, io};

use tracing::{debug, info, span, trace, warn, Level};

use pubsub_common::{
    ClientId, GetResponse, Message, PutResponse, Request, SequenceNumber, SequentialMessage,
    SubscribeResponse, Topic, UnsubscribeResponse,
};

pub struct Server {
    socket: zmq::Socket,
    queue: HashMap<(ClientId, Topic), VecDeque<Rc<Vec<u8>>>>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    client_get_sequences: HashMap<Topic, HashMap<ClientId, SequenceNumber>>,
    client_put_sequences: HashMap<Topic, HashMap<ClientId, SequenceNumber>>,
}

impl Server {
    pub fn new(port: u16) -> Result<Self, zmq::Error> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SocketType::REP)?;
        socket.bind(&format!("tcp://*:{}", port))?;
        info!("listening on port {}", port);

        Ok(Self::read_server_state_from_disk(socket))
    }

    pub fn run(&mut self) {
        let mut message = zmq::Message::new();
        loop {
            let span = span!(Level::DEBUG, "request");
            let _enter = span.enter();

            self.socket.recv(&mut message, 0).expect("recv failed");
            debug!("received request");

            let request: Request =
                serde_json::from_slice(&message).expect("request parsing failed");
            trace!(?request);

            let response = match request {
                Request::Get(s, t, sn) => serde_json::to_vec(&self.get(s, t, sn)),
                Request::Put(m, i, s) => serde_json::to_vec(&self.put(m, i, s)),
                Request::Subscribe(s, t) => serde_json::to_vec(&self.subscribe(s, t)),
                Request::Unsubscribe(s, t) => serde_json::to_vec(&self.unsubscribe(s, t)),
            }
            .unwrap();

            debug!("sending response");
            self.socket.send(response, 0).expect("send failed");
        }
    }

    fn put(
        &mut self,
        message: Message,
        publisher_id: ClientId,
        client_sequence_number: SequenceNumber,
    ) -> PutResponse {
        let span = span!(Level::DEBUG, "put");
        let _enter = span.enter();

        let server_side_sequence_number = self
            .client_put_sequences
            .entry(message.topic.clone())
            .or_default()
            .entry(publisher_id)
            .or_insert(0);
        trace!(client_sequence_number, server_side_sequence_number);

        match client_sequence_number.cmp(server_side_sequence_number) {
            Ordering::Greater => {
                debug!(
                    expected = server_side_sequence_number,
                    received = client_sequence_number,
                    "received message is a few messages ahead of what was expected"
                );
                return PutResponse::InvalidSequenceNumber(*server_side_sequence_number);
            }
            Ordering::Less => {
                debug!(
                    seq = client_sequence_number,
                    "client sent an already received message"
                );
                return PutResponse::RepeatedMessage(*server_side_sequence_number);
            }
            Ordering::Equal => {
                *server_side_sequence_number += 1;

                trace!("writing put sequences to disk");
                Self::write_put_sequences_to_disk(&self.client_put_sequences);
            }
        }

        match self.subscriptions.get(&message.topic) {
            Some(subscriber_set) => {
                let data_rc = Rc::new(message.data);
                debug!(message = ?data_rc, "storing message");

                for subscriber in subscriber_set {
                    trace!(subscriber, "adding message to queue");
                    self.queue
                        .get_mut(&(subscriber.to_owned(), message.topic.to_owned()))
                        .expect("subscriber does not have a message queue")
                        .push_back(data_rc.clone());

                    trace!("writing queue state to disk");
                    Self::write_queue_to_disk(&self.queue, &message.topic, subscriber);
                }
            }
            None => debug!(
                topic = message.topic,
                "topic has no subscribers, dropping message"
            ),
        }

        PutResponse::Ok
    }

    fn get(
        &mut self,
        subscriber: ClientId,
        topic: Topic,
        requested_get_sequence_number: SequenceNumber,
    ) -> GetResponse {
        let span = span!(Level::DEBUG, "get");
        let _enter = span.enter();

        let server_side_get_sequence_number = self
            .client_get_sequences
            .entry(topic.to_owned())
            .or_insert_with(HashMap::new)
            .get(&subscriber.to_owned())
            .unwrap_or(&0);
        let is_first_message = *server_side_get_sequence_number == 0;
        let requesting_last_message =
            requested_get_sequence_number + 1 == *server_side_get_sequence_number;
        let requesting_new_message =
            requested_get_sequence_number == *server_side_get_sequence_number;

        if !requesting_last_message && !requesting_new_message {
            debug!(
                subscriber,
                topic,
                requested_get_sequence_number,
                server_side_get_sequence_number,
                "client requested an invalid get sequence number"
            );
            return GetResponse::InvalidSequenceNumber(*server_side_get_sequence_number);
        }

        if !is_first_message && requesting_new_message {
            match self
                .queue
                .get_mut(&(subscriber.to_owned(), topic.to_owned()))
            {
                Some(queue) => {
                    if queue.len() <= 1 {
                        return GetResponse::NoMessageAvailable;
                    }
                    queue.pop_front();
                    debug!(subscriber, topic, "removed acknowledged message from queue");
                    trace!("writing queue state to disk");
                }
                _ => {}
            }
        }

        match self.queue.get(&(subscriber.to_owned(), topic.to_owned())) {
            Some(queue) => match queue.front() {
                Some(data) => {
                    debug!(
                        requested_get_sequence_number,
                        subscriber, topic, "incrementing get sequence number"
                    );
                    self.client_get_sequences
                        .get_mut(&topic)
                        .unwrap()
                        .insert(subscriber.to_owned(), requested_get_sequence_number + 1);

                    trace!("writing get sequences to disk");
                    Self::write_get_sequences_to_disk(&self.client_get_sequences);

                    debug!(
                        subscriber,
                        topic,
                        ?data,
                        "removed message from queue to send to the client"
                    );
                    trace!("writing queue state to disk");
                    Self::write_queue_to_disk(&self.queue, &topic, &subscriber);

                    return GetResponse::Ok(SequentialMessage {
                        message: Message {
                            topic: topic.clone(),
                            data: data.to_vec(),
                        },
                        sequence_number: requested_get_sequence_number + 1,
                    });
                }
                None => {
                    debug!(subscriber, topic, "queue is empty");
                    GetResponse::NoMessageAvailable
                }
            },
            None => {
                debug!(subscriber, topic, "queue does not exist");
                return GetResponse::NoMessageAvailable;
            }
        }
    }

    fn subscribe(&mut self, subscriber: ClientId, topic: Topic) -> SubscribeResponse {
        let span = span!(Level::DEBUG, "subscribe");
        let _enter = span.enter();
        debug!(subscriber, topic, "subscribing to topic");

        self.queue
            .entry((subscriber.to_owned(), topic.to_owned()))
            .or_insert_with(VecDeque::new);
        let set = self
            .subscriptions
            .entry(topic.to_owned())
            .or_insert_with(HashSet::new);

        if set.insert(subscriber.to_owned()) {
            Self::write_subscriptions_to_disk(&self.subscriptions);
            SubscribeResponse::Ok
        } else {
            debug!("topic was already subscribed");
            SubscribeResponse::AlreadySubscribed
        }
    }

    fn unsubscribe(&mut self, subscriber: ClientId, topic: Topic) -> UnsubscribeResponse {
        let span = span!(Level::DEBUG, "unsubscribe");
        let _enter = span.enter();

        let set = match self.subscriptions.get_mut(&topic) {
            Some(set) => set,
            None => return UnsubscribeResponse::NotSubscribed,
        };

        if set.remove(&subscriber) {
            debug!(subscriber, topic, "unsubscribed from topic");
            Self::write_subscriptions_to_disk(&self.subscriptions);
            UnsubscribeResponse::Ok
        } else {
            debug!(
                subscriber,
                topic, "topic was not subscribed, cannot unsubscribe"
            );
            UnsubscribeResponse::NotSubscribed
        }
    }

    fn read_server_state_from_disk(socket: zmq::Socket) -> Server {
        let subscriptions: HashMap<Topic, HashSet<ClientId>> =
            match fs::File::open("server_data/subscribers.json") {
                Ok(file) => serde_json::from_reader(file).unwrap(),
                Err(err) if err.kind() == io::ErrorKind::NotFound => HashMap::new(),
                Err(err) => panic!("failed to open subscribers file: {}", err),
            };

        let client_put_sequences: HashMap<Topic, HashMap<ClientId, SequenceNumber>> =
            match fs::File::open("server_data/put_sequences.json") {
                Ok(file) => serde_json::from_reader(file).unwrap(),
                Err(err) if err.kind() == io::ErrorKind::NotFound => HashMap::new(),
                Err(err) => panic!("failed to open put sequences file: {}", err),
            };

        let client_get_sequences: HashMap<Topic, HashMap<ClientId, SequenceNumber>> =
            match fs::File::open("server_data/get_sequences.json") {
                Ok(file) => serde_json::from_reader(file).unwrap(),
                Err(err) if err.kind() == io::ErrorKind::NotFound => HashMap::new(),
                Err(err) => panic!("failed to open get sequences file: {}", err),
            };

        let mut queue: HashMap<(ClientId, Topic), VecDeque<Rc<Vec<u8>>>> = HashMap::new();
        if let Ok(dir) = fs::read_dir("server_data/queue") {
            for entry in dir {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => panic!("failed to read queue directory entry: {}", err),
                };

                let path = entry.path();
                let metadata = match fs::metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        warn!(
                            "failed to read metadata for file {}: {}",
                            path.display(),
                            err
                        );
                        continue;
                    }
                };

                if metadata.is_file() {
                    let file_name = match path.file_name() {
                        Some(file_name) => file_name.to_str().unwrap(),
                        None => {
                            warn!("failed to read file name for file {}", path.display());
                            continue;
                        }
                    };
                    let subscriber = match file_name.split_once('-') {
                        Some((subscriber, _)) => subscriber,
                        None => {
                            warn!("failed to parse file name for file {}", path.display());
                            continue;
                        }
                    };
                    let topic = match file_name.split_once('-') {
                        Some((_, topic)) => topic.split_once('.').unwrap_or(("", "")).0,
                        None => {
                            warn!("failed to parse file name for file {}", path.display());
                            continue;
                        }
                    };
                    let messages: VecDeque<Vec<u8>> =
                        match fs::File::open(format!("server_data/queue/{}", file_name)) {
                            Ok(file) => serde_json::from_reader(file).unwrap(),
                            Err(err) if err.kind() == io::ErrorKind::NotFound => VecDeque::new(),
                            Err(err) => panic!("failed to open put sequences file: {}", err),
                        };
                    queue.insert(
                        (subscriber.to_owned(), topic.to_owned()),
                        messages.iter().map(|m| Rc::new(m.to_vec())).collect(),
                    );
                }
            }
        }

        Server {
            socket,
            subscriptions,
            queue,
            client_put_sequences,
            client_get_sequences,
        }
    }

    fn write_subscriptions_to_disk(subscriptions: &HashMap<Topic, HashSet<ClientId>>) {
        fs::create_dir_all("server_data").unwrap();
        fs::write(
            "server_data/subscribers.json",
            serde_json::to_string(subscriptions).unwrap(),
        )
        .unwrap();
    }

    fn write_queue_to_disk(
        queue: &HashMap<(ClientId, Topic), VecDeque<Rc<Vec<u8>>>>,
        topic: &Topic,
        subscriber: &ClientId,
    ) {
        fs::create_dir_all("server_data/queue").unwrap();
        fs::write(
            format!("server_data/queue/{}-{}.json", subscriber, topic),
            serde_json::to_string(&queue.get(&(subscriber.to_owned(), topic.to_owned()))).unwrap(),
        )
        .unwrap();
    }

    fn write_put_sequences_to_disk(
        client_put_sequences: &HashMap<Topic, HashMap<ClientId, SequenceNumber>>,
    ) {
        fs::create_dir_all("server_data").unwrap();
        fs::write(
            "server_data/put_sequences.json",
            serde_json::to_string(client_put_sequences).unwrap(),
        )
        .unwrap();
    }

    fn write_get_sequences_to_disk(
        client_get_sequences: &HashMap<Topic, HashMap<ClientId, SequenceNumber>>,
    ) {
        fs::create_dir_all("server_data").unwrap();
        fs::write(
            "server_data/get_sequences.json",
            serde_json::to_string(client_get_sequences).unwrap(),
        )
        .unwrap();
    }
}
