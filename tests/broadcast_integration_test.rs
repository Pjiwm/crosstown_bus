use std::{error::Error, sync::Arc};

use borsh::{BorshDeserialize, BorshSerialize};
use crosstown_bus::{MessageHandler, CrosstownBus, HandleError, QueueProperties};

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct OrderCreatedEventMessage {
    pub order_id: String,
    pub product_description: String
}

pub struct EmailSenderHandler;

impl MessageHandler<OrderCreatedEventMessage> for EmailSenderHandler {
    fn handle(&self, message: Box<OrderCreatedEventMessage>) -> Result<(), HandleError> {
        println!("E-mail send to the user that purchased {:?}, ID {:?}", message.product_description, message.order_id);
        Ok(())
    }
    fn get_handler_action(&self) -> String {
        return "send_email".to_owned()
    }
}

pub struct DatabaseUpdaterHandler;

impl MessageHandler<OrderCreatedEventMessage> for DatabaseUpdaterHandler {
    fn handle(&self, message: Box<OrderCreatedEventMessage>) -> Result<(), HandleError> {
        println!("Updating database with order: {:?}", message.order_id);
        Ok(())
    }
    fn get_handler_action(&self) -> String {
        return "update_database".to_owned()
    }
}

#[test]
fn broadcast() -> Result<(), Box<dyn Error>> {
    let mut subscriber = CrosstownBus::new_broadcast_subscriber::<OrderCreatedEventMessage>("amqp://guest:guest@localhost:5672".to_owned())?;

    _ = subscriber.add_subscription("order_created".to_owned(), Arc::new(DatabaseUpdaterHandler));

    _ = subscriber.subscribe_registered_events(QueueProperties { auto_delete: false, durable: false, use_dead_letter: true });

    let mut subscriber2 =
        CrosstownBus::new_broadcast_subscriber::<OrderCreatedEventMessage>("amqp://guest:guest@localhost:5672".to_owned())?;

    _ = subscriber2.add_subscription("order_created".to_owned(), Arc::new(EmailSenderHandler));

    _ = subscriber2.subscribe_registered_events(QueueProperties { auto_delete: false, durable: false, use_dead_letter: true });

    let mut publisher = CrosstownBus::new_broadcast_publisher("amqp://guest:guest@localhost:5672".to_owned())?;
    _ = publisher.publish_event("order_created".to_owned(),
        OrderCreatedEventMessage {
            order_id: "gtr123".to_owned(),
            product_description: "Electric Guitar".to_owned()
        });
    _ = publisher.publish_event("order_created".to_owned(),
        OrderCreatedEventMessage {
            order_id: "bss001".to_owned(),
            product_description: "Bass".to_owned()
        });

    Ok(())
}