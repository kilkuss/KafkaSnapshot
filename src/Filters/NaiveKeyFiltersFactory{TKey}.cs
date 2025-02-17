﻿using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Filters;

/// <summary>
/// Simple key filter factory.
/// </summary>
/// <typeparam name="TKey">Message key type.</typeparam>
public class NaiveKeyFiltersFactory<TKey> : IKeyFiltersFactory<TKey> where TKey : notnull
{
    /// <inheritdoc/>
    public IDataFilter<TKey> Create(FilterType filterKeyType, KeyType keyType, TKey sample)
        =>
        (filterKeyType, keyType, sample) switch
        {
            (FilterType.None, _, _)
                    => _default,
            (FilterType.GreaterOrEquals, KeyType.Long, long longSample)
                    => (IDataFilter<TKey>)new CompareFilter<long>(longSample, greater: true),
            (FilterType.LessOrEquals, KeyType.Long, long longSample)
                    => (IDataFilter<TKey>)new CompareFilter<long>(longSample, greater: false),
            (FilterType.Equals, KeyType.Long or KeyType.String, _)
                    => new EqualsFilter<TKey>(sample),
            (FilterType.Equals, KeyType.Json, string json)
                    => (IDataFilter<TKey>)new JsonEqualsFilter(json),
            (FilterType.Contains, KeyType.String, string data)
                    => (IDataFilter<TKey>)new StringContainsFilter(data),

            _ => throw new ArgumentException($"Invalid filter type {filterKeyType} for key type {keyType}" +
                $" with sample type {typeof(TKey).Name}.", nameof(filterKeyType)),
        };

    private readonly DefaultFilter<TKey> _default = new();
}
