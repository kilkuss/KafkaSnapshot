﻿using Newtonsoft.Json.Linq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Export.Markers;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Serializer for data with keys of <typeparamref name="TKey"/> type.
/// </summary>
/// <typeparam name="TKey">Data key type.</typeparam>
public class OriginalKeySerializer<TKey> :
             JsonSerializerBase, ISerializer<TKey, string, OriginalKeyMarker>
{
    /// <summary>
    /// Creates <see cref="OriginalKeySerializer{TKey}"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
    public OriginalKeySerializer(ILogger<OriginalKeySerializer<TKey>> logger) : base(logger) { }


    private static object ProjectData(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<string>>> data, 
                bool exportRawMessage)
        => data.Select(x => new
        {
            x.Key,
            Value = exportRawMessage ? x.Value.Message : JToken.Parse(x.Value.Message),
            x.Value.Meta
        });

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data is null.</exception>
    public string Serialize(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<string>>> data, 
                bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(data);

        return SerializeData(ProjectData(data, exportRawMessage));
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data or stream is null.</exception>
    public void Serialize(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<string>>> data, 
                bool exportRawMessage, 
                Stream stream)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(stream);

        SerializeDataToStream(ProjectData(data, exportRawMessage), stream);
    }
}
