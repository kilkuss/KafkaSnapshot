﻿namespace KafkaSnapshot.Abstractions.Filters
{
    /// <summary>
    /// Key filter for loading data from Kafka.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public interface IKeyFilter<TKey> : IDataFilter<TKey> where TKey : notnull
    {
    }
}
