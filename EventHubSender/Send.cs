using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Security.KeyVault.Secrets;
using EventHubSender;
using Newtonsoft.Json;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;



SenderSettings options = new();
IHost host = GetConfig();

//var client = new SecretClient(vaultUri: new Uri("https://eventhubsnskeyvault.vault.azure.net/"), credential: new DefaultAzureCredential());
//KeyVaultSecret secret = client.GetSecret("eventHubsConnectionString");

//var eventHubsConnectionString = secret.Value;
//var eventHubName = "eventhubtopic1";

//string ComputerName = System.Net.Dns.GetHostName();
//var BatchSize = 1; //1, 10, 1000, 5000


string eventHubsConnectionString = "";
string storageConnectionString = "";

//Read connection strings from Config file - Test/Deubgging only
if (options.EventHubsConnectionStringOverride != "")
{
    eventHubsConnectionString = options.EventHubsConnectionStringOverride;
}
else //Read from Azure KeyVault
{
    var client = new SecretClient(vaultUri: new Uri(options.AkvUri), credential: new DefaultAzureCredential());

    KeyVaultSecret secret = client.GetSecret(options.EventHubsConnectionStringConfigSettingName);
    eventHubsConnectionString = secret.Value;
}


var eventHubName = options.EventHubName;
var BatchSize = options.BatchSize; //1, 10, 1000, 5000
var InterBatchSleepTime = options.InterBatchSleepTime;
var NumberOfMessagesToSend = options.NumberOfMessagesToSend; //-1 = run forever
var PartitionKey = options.PartitionKey;
string ComputerName = System.Net.Dns.GetHostName();

await sendMicroBatchPartitionKey();

//await host.RunAsync();

async Task sendMicroBatchPartitionKey()
{
    var producer = new EventHubProducerClient(eventHubsConnectionString, eventHubName);

    try
    {
        var batchOptions = new CreateBatchOptions
        {
            PartitionKey = options.PartitionKey
        };

        //for (var counter = 0; counter < int.MaxValue; ++counter)
        //for (var counter = 0; counter < 5; ++counter)
        for (var counter = 0; counter < (options.NumberOfMessagesToSend == -1 ? int.MaxValue: options.NumberOfMessagesToSend); ++counter)
    
        {
            using EventDataBatch eventBatch = await producer.CreateBatchAsync(batchOptions);
            var eventData = new EventData();

            for (var index = 0; index < options.BatchSize; ++index)
            {

                var ProducerSentTimestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff");

                var eventDataJson = new[] { new { PartitionKey = batchOptions.PartitionKey, Host= ComputerName, Event = index, Id = Guid.NewGuid(), Timestamp = ProducerSentTimestamp} };
                var jsonData = JsonConvert.SerializeObject(eventDataJson);

                eventData = new EventData(jsonData);
                eventData.MessageId = Guid.NewGuid().ToString();
                eventData.CorrelationId = Guid.NewGuid().ToString();
                eventData.Properties.Add("BusinessEvent", "KYC.Complete");
                eventData.Properties.Add("Diagnostics.ProducerSentTimestamp", ProducerSentTimestamp);

                Console.WriteLine(jsonData);

                if (!eventBatch.TryAdd(eventData))
                {
                    break;
                }
            }
            await producer.SendAsync(eventBatch);
            Console.WriteLine($"POST: Sent Micro Batch Payload.");
            if (options.InterBatchSleepTime > 0) { Thread.Sleep(options.InterBatchSleepTime); }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.ToString());
        // Transient failures will be automatically retried as part of the
        // operation. If this block is invoked, then the exception was either
        // fatal or all retries were exhausted without a successful publish.
    }
    finally
    {
        await producer.CloseAsync();
    }
}

//Read settings from Configuration file & run
IHost GetConfig() 
{
    //Read settings from appsettings.json file
    using IHost host = Host.CreateDefaultBuilder(args)
        .ConfigureAppConfiguration((hostingContext, configuration) =>
        {
            configuration.Sources.Clear();

            IHostEnvironment env = hostingContext.HostingEnvironment;

            configuration
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json",  optional:true, reloadOnChange: true);

            IConfigurationRoot configurationRoot = configuration.Build();

           
            configurationRoot.GetSection(nameof(SenderSettings))
                             .Bind(options);

            Console.WriteLine($"Azure Keyvalue URI={options.AkvUri}");
            Console.WriteLine($"MessageSendDelay={options.MessageSendDelay}");
            Console.WriteLine($"EventHubsConnectionStringConfigSettingName={options.EventHubsConnectionStringConfigSettingName}");
            Console.WriteLine($"EventHubName={options.EventHubName}");
            Console.WriteLine($"BatchSize={options.BatchSize}");
            Console.WriteLine($"InterBatchSleepTime={options.InterBatchSleepTime}");
            Console.WriteLine($"NumberOfMessagesToSend={options.NumberOfMessagesToSend}");
            Console.WriteLine($"PartitionKey={options.PartitionKey}");
            Console.WriteLine($"EventHubsConnectionStringOverride={options.EventHubsConnectionStringOverride}");

            
        })
        .Build();

    return host;
}
