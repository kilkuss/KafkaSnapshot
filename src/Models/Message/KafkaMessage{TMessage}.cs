﻿namespace KafkaSnapshot.Models.Message;

/// <summary>
/// Kafka message with metadata.
/// </summary>
/// <typeparam name="TMessage">Message type.</typeparam>
/// <param name="Message">Kafka message.</param>
/// <param name="Meta">Message metadata.</param>
public record KafkaMessage<TMessage>(TMessage Message, KafkaMetadata Meta)
    where TMessage : notnull;
