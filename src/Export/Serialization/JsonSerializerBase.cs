﻿using System.Diagnostics;
using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Base class for Json serialization.
/// </summary>
public abstract class JsonSerializerBase
{
    /// <summary>
    /// Creates <see cref="JsonSerializerBase"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
    public JsonSerializerBase(ILogger<JsonSerializerBase> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected string SerializeData(object data)
    {
        Debug.Assert(data is not null);

        var sb = new StringBuilder();

        using var textWriter = new StringWriter(sb);

        using var jsonWriter = new JsonTextWriter(textWriter);

        _logger.LogTrace("Start serializing data.");

        _serializer.Serialize(jsonWriter, data);

        _logger.LogTrace("Finish serializing data.");

        return sb.ToString(); //TODO Change on Stream. OOM on big count of data.
    }

    protected void SerializeDataToStream(object data, Stream stream)
    {
        Debug.Assert(data is not null);
        Debug.Assert(stream is not null);

        using var sw = new StreamWriter(stream);
        using var jsonWriter = new JsonTextWriter(sw);

        _logger.LogTrace("Start serializing data.");

        _serializer.Serialize(jsonWriter, data);

        _logger.LogTrace("Finish serializing data.");
    }

    protected static object SerializeValueMassage<TMessage>(
        TMessage message, 
        bool exportRawMessage)
    {
        ArgumentNullException.ThrowIfNull(message);
        
        if (typeof(TMessage) == typeof(byte[]))
        {
            var bytes = message as byte[];
            if (exportRawMessage)
            {
                return bytes!;
            }

            return MessagePackSerializer.ConvertToJson(
                bytes,
                MessagePackSerializerOptions.Standard.WithCompression(
                    MessagePackCompression.Lz4Block));
        }

        if (typeof(TMessage) == typeof(string))
        {
            var stringMassage = message as string;
            if (exportRawMessage)
            {
                return stringMassage!;
            }

            return JToken.Parse(stringMassage!);
        }

        throw new InvalidOperationException("Not support value massage type.");
    }

    private readonly JsonSerializer _serializer = new() { Formatting = Formatting.Indented };
    protected readonly ILogger<JsonSerializerBase> _logger;
}
