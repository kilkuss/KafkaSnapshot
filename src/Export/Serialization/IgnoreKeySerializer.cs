using Newtonsoft.Json.Linq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Serializer for data with no keys.
/// </summary>
public class IgnoreKeySerializer<TMessage> : JsonSerializerBase, ISerializer<string, TMessage, IgnoreKeyMarker> 
    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="IgnoreKeySerializer"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
    public IgnoreKeySerializer(ILogger<IgnoreKeySerializer<TMessage>> logger) : base(logger) { }

    private object ProjectData(
                    IEnumerable<KeyValuePair<string, KafkaMessage<TMessage>>> data, 
                    bool exportRawMessage)
    {
        if (data.Any(x => x.Key is not null))
        {
            _logger.LogWarning("Serialization data contains not null Keys. " +
                "This Keys will be ignored.");
        }

        var items = data.Select(x => new
        {
            Value = SerializeValueMassage(x.Value.Message, exportRawMessage),
            x.Value.Meta
        });

        return items;
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data is null.</exception>
    public string Serialize(
            IEnumerable<KeyValuePair<string, KafkaMessage<TMessage>>> data, 
            bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(data);

        return SerializeData(ProjectData(data, exportRawMessage));
    }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown when data or stream is null.</exception>
    public void Serialize(
            IEnumerable<KeyValuePair<string, KafkaMessage<TMessage>>> data, 
            bool exportRawMessage, 
            Stream stream)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(stream);

        SerializeDataToStream(ProjectData(data, exportRawMessage), stream);
    }
}
