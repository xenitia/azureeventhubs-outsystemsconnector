using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Messaging.EventHubs.Primitives;
using MFEventHubProcessor;
using Azure.Security.KeyVault.Secrets;
using Azure.Identity;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using EventHubReceiver;

ReceiverSettings options = new();
IHost host = GetConfig();



//var client = new SecretClient(vaultUri: new Uri(options.AkvUri), credential: new DefaultAzureCredential());
//KeyVaultSecret secret = client.GetSecret(options.EventHubsConnectionStringConfigSettingName);
//var eventHubsConnectionString = secret.Value;
//var eventHubName = options.EventHubName;
//var BatchSize = options.BatchSize; //1, 10, 1000, 5000
//var InterBatchSleepTime = options.InterBatchSleepTime;
//var NumberOfMessagesToSend = options.NumberOfMessagesToSend; //-1 = run forever
//var PartitionKey = options.PartitionKey;
//string ComputerName = System.Net.Dns.GetHostName();

string eventHubsConnectionString = "";
string storageConnectionString = "";

//Read connection strings from Config file - Test/Deubgging only
if (options.EventHubsConnectionStringOverride != "")
{
    eventHubsConnectionString = options.EventHubsConnectionStringOverride;
    storageConnectionString = options.StorageAccountConnectionStringOverride;
}
else //Read from Azure KeyVault
{
    var client = new SecretClient(vaultUri: new Uri(options.AkvUri), credential: new DefaultAzureCredential());

    KeyVaultSecret secret = client.GetSecret(options.EventHubsConnectionStringConfigSettingName);
    eventHubsConnectionString = secret.Value;
    secret = client.GetSecret(options.StorageAccountConnectionStringConfigSettingName);
    storageConnectionString = secret.Value;
}
var blobContainerName = options.StorageAccountBlobContainerNameConfigSettingName;

var eventHubName = options.EventHubName;
var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

var storageClient = new BlobContainerClient(
    storageConnectionString,
    blobContainerName);

var maximumBatchSize = options.MaximumBatchSize;

var processor = new MFCustomEventHubProcessor(
    storageClient,
    maximumBatchSize,
    consumerGroup,
    eventHubsConnectionString,
    eventHubName);

using var cancellationSource = new CancellationTokenSource();
//cancellationSource.CancelAfter(TimeSpan.FromSeconds(30));
cancellationSource.CancelAfter(Timeout.Infinite);

// Starting the processor does not block when starting; delay
// until the cancellation token is signaled.

try
{
    await processor.StartProcessingAsync(cancellationSource.Token);
    await Task.Delay(Timeout.Infinite, cancellationSource.Token);
    //await Task.Delay(TimeSpan.FromSeconds(30), cancellationSource.Token);

    //await host.RunAsync(cancellationSource.Token);

}
catch (TaskCanceledException)
{
    // This is expected if the cancellation token is
    // signaled.
}
finally
{
    // Stopping may take up to the length of time defined
    // as the TryTimeout configured for the processor;
    // By default, this is 60 seconds.
    await processor.StopProcessingAsync();
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
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configurationRoot = configuration.Build();


            configurationRoot.GetSection(nameof(ReceiverSettings))
                             .Bind(options);

            Console.WriteLine($"Azure Keyvalue URI={options.AkvUri}");
            Console.WriteLine($"EventHubsConnectionStringConfigSettingName={options.EventHubsConnectionStringConfigSettingName}");
            Console.WriteLine($"EventHubName={options.EventHubName}");
            Console.WriteLine($"BatchSize={options.MaximumBatchSize}");

            Console.WriteLine($"EventHubsConnectionStringOverride={options.EventHubsConnectionStringOverride}");
            Console.WriteLine($"StorageAccountConnectionStringOverride={options.StorageAccountConnectionStringOverride}");

        })
        .Build();

    return host;
}
