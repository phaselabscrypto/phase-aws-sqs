use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_sqs::{
    Client, Error
};
use async_trait::async_trait;
use std::sync::Arc;

/// Represents a client that interacts with AWS SQS.
/// The client is responsible for sending and consuming messages from an SQS queue.
///
/// The `SqsClient` is generic over a `MessageProcessor` type, which is used to process messages
/// when consuming them from the queue.
pub struct SqsClient<T>
where
    T: MessageProcessor + Send + Sync,
{
    sqs_client: Client,
    queue_url: String,
    message_processor: Arc<T>,
}

impl<T> SqsClient<T>
where
    T: MessageProcessor + Send + Sync,
{
    /// Creates a new `SqsClient` instance.
    ///
    /// # Arguments
    ///
    /// * `queue_url` - The URL of the SQS queue to interact with.
    /// * `processor` - An `Arc` to a type implementing the `MessageProcessor` trait, used to process messages.
    /// * `region` - An optional AWS region to use. If `None`, the default region from the environment or SDK is used.
    ///
    /// # Returns
    ///
    /// A new instance of `SqsClient`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let processor = Arc::new(MyMessageProcessor);
    /// let client = SqsClient::new("https://sqs.us-east-2.amazonaws.com/123456789/my-queue".to_string(), processor, None).await;
    /// ```
    pub async fn new(
        queue_url: String,
        processor: Arc<T>,
        region: Option<Region>,
    ) -> Self {
        // Use the region provided or default to the environment/SDK region
        let region_provider = RegionProviderChain::first_try(region)
            .or_default_provider()
            .or_else(Region::new("us-east-2"));

        let config = aws_config::defaults(BehaviorVersion::latest()).region(region_provider).load().await;

        let sqs_client = Client::new(&config);

        Self {
            sqs_client,
            queue_url,
            message_processor: processor,
        }
    }

    /// Consumes messages from the SQS queue and processes them using the provided `MessageProcessor`.
    ///
    /// The method retrieves messages from the queue, then passes each message body to the
    /// `process_message` method of the `MessageProcessor`.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQS client fails to receive messages from the queue.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let result = client.consume_messages().await;
    /// if result.is_ok() {
    ///     println!("Messages processed successfully.");
    /// }
    /// ```
    pub async fn consume_messages(&self) -> Result<(), Error> {
        let response = self
            .sqs_client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(10)
            .send()
            .await?;

        if let Some(messages) = response.messages {
            for message in messages {
                if let Some(body) = message.body {
                    self.message_processor.process_message(body).await;
                }
            }
        }

        Ok(())
    }

    /// Sends a message to the SQS queue.
    ///
    /// # Arguments
    ///
    /// * `message_body` - The content of the message to be sent to the SQS queue.
    ///
    /// # Errors
    ///
    /// Returns an error if the message fails to send.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let result = client.send_message("My message".to_string()).await;
    /// if result.is_ok() {
    ///     println!("Message sent successfully.");
    /// }
    /// ```
    pub async fn send_message(&self, message_body: String) -> Result<(), Error> {
        self.sqs_client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(message_body)
            .send()
            .await?;

        Ok(())
    }
}

/// A trait that defines how to process messages from the SQS queue.
///
/// Types implementing this trait can be used as the `MessageProcessor` in `SqsClient`.
#[async_trait]
pub trait MessageProcessor {
    /// Processes a message retrieved from the SQS queue.
    ///
    /// # Arguments
    ///
    /// * `message` - The body of the message retrieved from the queue.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// struct MyProcessor;
    /// 
    /// #[async_trait]
    /// impl MessageProcessor for MyProcessor {
    ///     async fn process_message(&self, message: String) {
    ///         println!("Processing message: {}", message);
    ///     }
    /// }
    /// ```
    async fn process_message(&self, message: String);
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_sqs::operation::{
        send_message::SendMessageError,
        receive_message::ReceiveMessageError,
    };
    use mockall::*;
    use mockall::predicate::*;

    pub struct LoggerProcessor;

    #[async_trait]
    impl MessageProcessor for LoggerProcessor {
        async fn process_message(&self, message: String) {
            println!("Logging message: {}", message);
        }
    }

    #[tokio::test]
    async fn test_logger_processor() {
        let processor = LoggerProcessor;
        processor.process_message("Test log message".to_string()).await;
    }

    mock! {
        pub SqsClient<T> where T: MessageProcessor + Send + Sync {
            pub async fn send_message(&self, message_body: String) -> Result<(), Error>;
            pub async fn consume_messages(&self) -> Result<(), Error>;
        }
    }

    #[tokio::test]
    async fn test_send_message_success() {
        let mut mock_sqs_client = MockSqsClient::<LoggerProcessor>::new();

        mock_sqs_client
            .expect_send_message()
            .with(eq("Test message".to_string()))
            .returning(|_| Ok(()));

        let result = mock_sqs_client.send_message("Test message".to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_send_message_failure() {
        let mut mock_sqs_client = MockSqsClient::<LoggerProcessor>::new();

        mock_sqs_client
            .expect_send_message()
            .with(eq("Test message".to_string()))
            .returning(|_| Err(Error::from(SendMessageError::unhandled("Simulated error"))));

        let result = mock_sqs_client.send_message("Test message".to_string()).await;
        assert!(result.is_err());
    }
    #[tokio::test]
    async fn test_consume_messages_success() {
        let mut mock_sqs_client = MockSqsClient::<LoggerProcessor>::new();

        mock_sqs_client
            .expect_consume_messages()
            .returning(|| Ok(()));

        let result = mock_sqs_client.consume_messages().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_consume_messages_failure() {
        let mut mock_sqs_client = MockSqsClient::<LoggerProcessor>::new();

        mock_sqs_client
            .expect_consume_messages()
            .returning(|| Err(Error::from(ReceiveMessageError::unhandled("Simulated consume error"))));

        let result = mock_sqs_client.consume_messages().await;
        assert!(result.is_err());
    }
}