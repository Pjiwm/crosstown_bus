use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::{error::Error, sync::Arc};

use borsh::{BorshDeserialize, BorshSerialize};
#[derive(Clone, Debug)]
pub struct Subscriber<T: Debug + Clone> {
    subscribed: RefCell<Option<T>>,
}

impl<T: Debug + Clone> Subscriber<T> {
    pub fn new() -> Self {
        Self {
            subscribed: RefCell::new(None),
        }
    }

    pub fn get_subscribed(&self) -> Option<T> {
        self.subscribed.borrow().clone()
    }
}

impl<T: Debug + Clone> std::fmt::Display for Subscriber<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T: Debug + Clone> MessageHandler<T> for Subscriber<T> {
    fn get_handler_action(&self) -> String {
        String::from(format!("{:?}", self))
    }

    fn handle(&self, message: Box<T>) -> Result<(), HandleError>
    where
        T: Clone + BorshDeserialize + BorshSerialize + 'static,
    {
        self.subscribed.replace(Some(*message));
        Ok(())
    }
}

impl<T: Debug + Clone> MessageHandler<T> for Arc<Subscriber<T>> {
    fn get_handler_action(&self) -> String {
        String::from(format!("{:?}", self))
    }

    fn handle(&self, message: Box<T>) -> Result<(), HandleError>
    where
        T: Clone + BorshDeserialize + BorshSerialize + 'static,
    {
        self.subscribed.replace(Some(*message));
        Ok(())
    }
}

pub trait MessageHandler<T> {
    fn get_handler_action(&self) -> String;
    fn handle(&self, message: Box<T>) -> Result<(), HandleError>
    where
        T: Clone + BorshDeserialize + BorshSerialize + 'static;
}

#[derive(Debug)]
pub struct HandleError {
    details: String,
    pub requeue: bool,
}

impl HandleError {
    pub fn new(details: String, requeue: bool) -> Self {
        Self { details, requeue }
    }
}

impl Error for HandleError {
    fn description(&self) -> &str {
        &self.details
    }
}

impl fmt::Display for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/requeue:{}", self.details, self.requeue)
    }
}

pub(crate) fn send_message_to_handler<T>(
    delivery: amiquip::Delivery,
    handler: &Arc<impl MessageHandler<T> + Send + Sync>,
    channel: &amiquip::Channel,
) where
    T: BorshDeserialize + BorshSerialize + Clone + 'static,
{
    let str_message = String::from_utf8_lossy(&delivery.body).to_string();
    let mut buf = str_message.as_bytes();
    if let Ok(model) = BorshDeserialize::deserialize(&mut buf) {
        if let Err(err) = handler.handle(model) {
            println!("{err}");
            _ = delivery.nack(channel, err.requeue);
        } else {
            _ = delivery.ack(channel);
        }
    } else {
        _ = delivery.nack(channel, false);
        eprintln!(
            "[crosstown_bus] Error trying to desserialize. Check message format. Message: {:?}",
            str_message
        );
    }
}
