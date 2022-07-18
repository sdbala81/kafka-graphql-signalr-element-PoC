// See https://aka.ms/new-console-template for more information

using System.Globalization;
using System.Net;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using EventLogger.Core;
using EventLogger.Models;

await SendMessagesToKafkaTopic();


async Task SendMessagesToKafkaTopic()
{
    Console.WriteLine("Sending Messages to Kafka");


    var adminConfig = new AdminClientConfig { BootstrapServers = "127.0.0.1:9092" };
    var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://127.0.0.1:8081" };
    var producerConfig = new ProducerConfig
    {
        BootstrapServers = "127.0.0.1:9092",
        // Guarantees delivery of message to topic.
        EnableDeliveryReports = true,
        ClientId = Dns.GetHostName()
    };

    using var adminClient = new AdminClientBuilder(adminConfig).Build();
    try
    {
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = "leave-applications",
                ReplicationFactor = 1,
                NumPartitions = 3
            }
        });
    }
    catch (CreateTopicsException e) when (e.Results.Select(r => r.Error.Code)
                                              .Any(el => el == ErrorCode.TopicAlreadyExists))
    {
        Console.WriteLine($"Topic {e.Results[0].Topic} already exists");
    }
    
    
    using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
    
    using var producer = new ProducerBuilder<string, LeaveApplicationReceived>(producerConfig)
        .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
        .SetValueSerializer(new AvroSerializer<LeaveApplicationReceived>(schemaRegistry))
        .Build();
    
    while (true)
    {
        var empEmail = ReadLine.Read("Enter your employee Email (e.g. none@example-company.com): ",
            "none@example.com").ToLowerInvariant();
        var empDepartment = ReadLine.Read("Enter your department code (HR, IT, OPS): ").ToUpperInvariant();
        var leaveDurationInHours =
            int.Parse(ReadLine.Read("Enter number of hours of leave requested (e.g. 8): ", "8"));
        var leaveStartDate = DateTime.ParseExact(ReadLine.Read("Enter vacation start date (dd-mm-yy): ",
            $"{DateTime.Today:dd-MM-yy}"), "dd-mm-yy", CultureInfo.InvariantCulture);

        var leaveApplication = new LeaveApplicationReceived
        {
            EmpDepartment = empDepartment,
            EmpEmail = empEmail,
            LeaveDurationInHours = leaveDurationInHours,
            LeaveStartDateTicks = leaveStartDate.Ticks
        };
        var partition = new TopicPartition(
            ApplicationConstants.LeaveApplicationsTopicName,
            new Partition((int) Enum.Parse<Departments>(empDepartment)));
        
        var result = await producer.ProduceAsync(partition,
            new Message<string, LeaveApplicationReceived>
            {
                Key = $"{empEmail}-{DateTime.UtcNow.Ticks}",
                Value = leaveApplication
            });
        
        Console.WriteLine(
            $"\nMsg: Your leave request is queued at offset {result.Offset.Value} in the Topic {result.Topic}:{result.Partition.Value}\n\n");
    }
    
    
}