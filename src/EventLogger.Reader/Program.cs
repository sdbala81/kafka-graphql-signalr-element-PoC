// See https://aka.ms/new-console-template for more information

using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using EventLogger.Models;


ReadMessageFromKafka();

void ReadMessageFromKafka()
{

    Console.WriteLine("Reading from Kafka Stream");
    
    var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://127.0.0.1:8081" };
    
    var consumerConfig = new ConsumerConfig
    {
        BootstrapServers = "127.0.0.1:9092",
        GroupId = "manager",
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false,
        // Read messages from start if no commit exists.
        AutoOffsetReset = AutoOffsetReset.Earliest,
        MaxPollIntervalMs = 100000
    };

    
    
    var leaveApplicationReceivedMessages = new Queue<KafkaMessage>();

    using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
    
    using var consumer = new ConsumerBuilder<string, LeaveApplicationReceived>(consumerConfig)
        .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
        .SetValueDeserializer(new AvroDeserializer<LeaveApplicationReceived>(schemaRegistry).AsSyncOverAsync())
        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        .Build();
    {
        try
        {
            consumer.Subscribe("leave-applications");
            Console.WriteLine("Consumer loop started...\n");
            while (true)
            {
                try
                {
                    // We will give the process 1 second to commit the message and store its offset.
                    var result = consumer.Consume();
                    var leaveRequest = result?.Message?.Value;
                    if (leaveRequest == null)
                    {
                        continue;
                    }

                    // Adding message to a list just for the demo.
                    // You should persist the message in database and process it later.
                    leaveApplicationReceivedMessages.Enqueue(new KafkaMessage(result.Message.Key, result.Partition.Value, result.Message.Value));

                    Console.WriteLine(
                        $"Received message: {result.Message.Key} Value: {JsonSerializer.Serialize(leaveRequest)}");
                    
                    consumer.Commit(result);
                    consumer.StoreOffset(result);
                }
                catch (ConsumeException e) when (!e.Error.IsFatal)
                {
                    Console.WriteLine($"Non fatal error: {e}");
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }
    
}

public record KafkaMessage(string Key, int Partition, LeaveApplicationReceived Message);