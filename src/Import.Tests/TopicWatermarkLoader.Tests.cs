﻿using Microsoft.Extensions.Options;

using Confluent.Kafka;

using FluentAssertions;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

namespace KafkaAsTable.Tests;

public class TopicWatermarkLoaderTests
{
    [Fact(DisplayName = "TopicWatermarkLoader can be created with valid params.")]
    [Trait("Category", "Unit")]
    public void TopicWatermarkLoaderCanBeCreated()
    {

        // Arrange
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {

        });

        // Act
        var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't be created with null client.")]
    [Trait("Category", "Unit")]
    public void TopicWatermarkLoaderCantBeCreatedWithNullClient()
    {

        // Arrange
        var client = (IAdminClient)null!;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {

        });

        // Act
        var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't be created with null options.")]
    [Trait("Category", "Unit")]
    public void TopicWatermarkLoaderCantBeCreatedWithNullOptions()
    {

        // Arrange
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (IOptions<TopicWatermarkLoaderConfiguration>)null!;

        // Act
        var exception = Record.Exception(() => new TopicWatermarkLoader(client, options));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't be created with null options value.")]
    [Trait("Category", "Unit")]
    public void TopicWatermarkLoaderCantBeCreatedWithNullOptionsValue()
    {

        // Arrange
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.SetupGet(x => x.Value).Returns(() => null!);

        // Act
        var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't load data with null factory.")]
    [Trait("Category", "Unit")]
    public async Task TopicWatermarkLoaderCantLoadWithNullFactory()
    {

        // Arrange
        using var cts = new CancellationTokenSource();
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {

        });
        var loader = new TopicWatermarkLoader(client, options.Object);
        var consumerFactory = (Func<IConsumer<string, string>>)null!;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), partitionFilter);

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        await loader.LoadWatermarksAsync(consumerFactory, topicName, cts.Token).ConfigureAwait(false)
        );

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't load data with null topic.")]
    [Trait("Category", "Unit")]
    public async Task TopicWatermarkLoaderCantLoadWithNullTopic()
    {

        // Arrange
        using var cts = new CancellationTokenSource();
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {

        });
        var loader = new TopicWatermarkLoader(client, options.Object);
        static IConsumer<string, string> consumerFactory() => null!;
        var topicName = (LoadingTopic)null!;

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        await loader.LoadWatermarksAsync(consumerFactory, topicName, cts.Token).ConfigureAwait(false)
        );

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can load watermarks with valid params.")]
    [Trait("Category", "Unit")]
    public async Task CanLoadWatermarksWithValidParams()
    {

        //Arrange
        using var cts = new CancellationTokenSource();
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), partitionFilter);
        var clientMock = new Mock<IAdminClient>(MockBehavior.Strict);
        var client = clientMock.Object;
        var timeout = 1;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(timeout)
        });
        var loader = new TopicWatermarkLoader(client, options.Object);
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        IConsumer<object, object> consumerFactory() => consumerMock!.Object;
        var adminClientPartition = new TopicPartition(topic.Value.Name, new Partition(1));
        var adminParitions = new[] { adminClientPartition };
        var borkerMeta = new BrokerMetadata(1, "testHost", 1000);
        var partitionMeta = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);
        var topicMeta = new TopicMetadata(topic.Value.Name, new[] { partitionMeta }.ToList(), null);
        var meta = new Confluent.Kafka.Metadata(
                new[] { borkerMeta }.ToList(),
                new[] { topicMeta }.ToList(), 1, "test"
                );
        clientMock.Setup(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout))).Returns(meta);
        var offets = new WatermarkOffsets(new Offset(1), new Offset(2));
        consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition, TimeSpan.FromSeconds(timeout))).Returns(offets);
        consumerMock.Setup(x => x.Dispose());
        consumerMock.Setup(x => x.Close());

        // Act
        var result = await loader.LoadWatermarksAsync(consumerFactory, topic, cts.Token).ConfigureAwait(false);

        // Assert
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
        result.Should().NotBeNull();
        var watermarks = result.Watermarks.ToList();
        watermarks.Should().ContainSingle();
        clientMock.Verify(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout)), Times.Once);
        consumerMock.Verify(x => x.QueryWatermarkOffsets(adminClientPartition, TimeSpan.FromSeconds(timeout)), Times.Once);
        watermarks.Single().TopicName.Should().Be(topic);
        watermarks.Single().Partition.Value.Should().Be(partitionMeta.PartitionId);
        watermarks.Single().Offset.Should().Be(offets);
    }

    [Fact(DisplayName = "TopicWatermarkLoader can load watermarks with valid params with partition filter.")]
    [Trait("Category", "Unit")]
    public async Task CanLoadWatermarksWithValidParamsWithPartitionFilter()
    {
        //Arrange
        using var cts = new CancellationTokenSource();
        var topic = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), new HashSet<int>(new[] { 2 }));
        var clientMock = new Mock<IAdminClient>(MockBehavior.Strict);
        var client = clientMock.Object;
        var timeout = 1;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(timeout)
        });
        var loader = new TopicWatermarkLoader(client, options.Object);
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        IConsumer<object, object> consumerFactory() => consumerMock!.Object;
        var adminClientPartition1 = new TopicPartition(topic.Value.Name, new Partition(1));
        var adminClientPartition2 = new TopicPartition(topic.Value.Name, new Partition(2));
        var adminParitions = new[] { adminClientPartition1, adminClientPartition2 };
        var borkerMeta = new BrokerMetadata(1, "testHost", 1000);
        var partitionMeta1 = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);
        var partitionMeta2 = new PartitionMetadata(2, 1, new[] { 1 }, new[] { 1 }, null);
        var topicMeta = new TopicMetadata(topic.Value.Name, new[] { partitionMeta1, partitionMeta2 }.ToList(), null);
        var meta = new Confluent.Kafka.Metadata(
                new[] { borkerMeta }.ToList(),
                new[] { topicMeta }.ToList(), 1, "test"
                );
        clientMock.Setup(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout))).Returns(meta);
        var offets = new WatermarkOffsets(new Offset(1), new Offset(2));
        consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition1, TimeSpan.FromSeconds(timeout))).Returns(offets);
        consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition2, TimeSpan.FromSeconds(timeout))).Returns(offets);
        consumerMock.Setup(x => x.Dispose());
        consumerMock.Setup(x => x.Close());

        // Act
        var result = await loader.LoadWatermarksAsync(consumerFactory, topic, cts.Token).ConfigureAwait(false);

        // Assert
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
        result.Should().NotBeNull();
        var watermarks = result.Watermarks.ToList();
        watermarks.Should().ContainSingle();
        clientMock.Verify(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout)), Times.Once);
        consumerMock.Verify(x => x.QueryWatermarkOffsets(adminClientPartition1, TimeSpan.FromSeconds(timeout)), Times.Never);
        consumerMock.Verify(x => x.QueryWatermarkOffsets(adminClientPartition2, TimeSpan.FromSeconds(timeout)), Times.Once);
        watermarks.Single().TopicName.Should().Be(topic);
        watermarks.Single().Partition.Value.Should().Be(partitionMeta2.PartitionId);
        watermarks.Single().Offset.Should().Be(offets);
    }
}
