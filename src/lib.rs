//! # Crosstown Bus
//!
//! `crosstown_bus` is an easy-to-configure bus in Rust with RabbitMQ for event-driven systems.

use std::{error::Error, thread};
use serde_json::json;

use amiquip::{Connection, QueueDeclareOptions, ConsumerMessage, ConsumerOptions, FieldTable, ExchangeDeclareOptions, Publish};

type HandleResult = Result<(), Box<dyn Error>>;
type SubscribeResult = Result<(), Box<dyn Error>>;
type PublishResult = Result<(), Box<dyn Error>>;

pub struct Bus {
    url: String
}

impl Bus {
    
    pub fn new_rabbit_bus(url: String) -> Result<Bus, Box<dyn Error>> {
        Ok(Bus { url })
    }

    /// Publishes an event.
    ///
    /// # Example
    /// ```
    /// use crosstown_bus::{Bus};
    /// use serde::Serialize;
    /// use serde::Deserialize;
    /// 
    /// #[derive(Serialize, Deserialize)]
    /// pub struct UserCreated {
    ///     name: String,
    ///     id: String
    /// }

    /// let bus = Bus::new_rabbit_bus("amqp://guest:guest@localhost:5672".to_string()).unwrap();

    /// let _ = bus.subscribe_event::<UserCreated>(String::from("send_email"), |message| {
    ///     println!("User CREATED! e-mail sent now: {}", message);
    ///     (false, Ok(()))
    /// });

    /// let res = bus.publish_event::<UserCreated>(UserCreated {
    ///     name: "Paolo".to_owned(),
    ///     id: "F458asYfj".to_owned()
    /// });
    /// ```
    pub fn publish_event<T>(&self, message: T) -> PublishResult 
        where T: serde::ser::Serialize {
        let url = self.url.to_owned();
        let event_name = get_event_name::<T>();

        let message_json = json!(message).to_string();

        if let Ok(channel) = Connection::insecure_open(&url)?.open_channel(None) {
            let publish_result = channel.basic_publish::<String>(event_name.to_owned(), Publish {
                body: message_json.as_bytes(),
                routing_key: event_name.to_owned(),
                mandatory: false,
                immediate: false,
                properties: Default::default(),
            });
            if publish_result.is_err() {
                return Err(Box::new(publish_result.unwrap_err()));
            }
        }
        Ok(())
    }

    /// Subscribes to an event
    pub fn subscribe_event<T>(&self, action_name: String, handler: fn(message: String) -> (bool, HandleResult)) -> SubscribeResult {
        let url = self.url.to_owned();

        let event_name = get_event_name::<T>();

        let mut queue_name = event_name.to_owned();
        queue_name.push_str(&String::from("."));
        queue_name.push_str(&action_name);

        thread::spawn(move || {
            match Connection::insecure_open(&url) {
                Ok(mut cnn) => {
                    if let Ok(channel) = cnn.open_channel(None) {
                        match channel.queue_declare(queue_name.to_owned(), QueueDeclareOptions {
                            durable: false,
                            exclusive: false,
                            auto_delete: false,
                            ..Default::default()
                        }) {
                            Ok(queue) => {
                                let exchange_declare_options = ExchangeDeclareOptions {
                                    auto_delete: false,
                                    durable: false,
                                    internal: false,
                                    ..Default::default()
                                };
            
                                let _ = queue.bind(
                                    &channel.exchange_declare::<String>(amiquip::ExchangeType::Fanout, event_name.to_owned(), exchange_declare_options).unwrap(), 
                                    "".to_string(), FieldTable::new());
            
                                if let Ok(consumer) = queue.consume(ConsumerOptions::default()) {
                                    for message in consumer.receiver().iter() {
                                        match message {
                                            ConsumerMessage::Delivery(delivery) => {
                                                let body = String::from_utf8_lossy(&delivery.body);
                
                                                let handle_result = handler(body.to_string());

                                                let retry_on_error = handle_result.0;
                                                let result = handle_result.1;

                                                if result.is_ok() {
                                                    let _ = delivery.ack(&channel);
                                                } else {
                                                    if retry_on_error {
                                                        let _ = delivery.nack(&channel, true);
                                                    } else {
                                                        let _ = delivery.reject(&channel, false);
                                                    }
                                                }
                                            }
                                            other => {
                                                println!("Consumer ended: {:?}", other);
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    eprintln!("[bus] Error trying to consume");
                                }
                            },
                            Err(err) => eprintln!("[bus] Error creating Queue: {:?}", err),
                        };
                    } else {
                        eprintln!("[bus] Error opening channel");
                    }
                },
                Err(err) => eprintln!("[bus] Error trying to create connection: {:?}", err),
            }
        });

        Ok(())
    }
}

fn get_event_name<T>() -> String {
    let full_event_name = std::any::type_name::<T>().to_string();
    let event_array = full_event_name.split("::").collect::<Vec<&str>>();
    let event_name = event_array.last().unwrap().to_string();
    event_name
}