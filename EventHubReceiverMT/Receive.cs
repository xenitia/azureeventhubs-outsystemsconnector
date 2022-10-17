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


var client = new SecretClient(vaultUri: new Uri("https://eventhubsnskeyvault.vault.azure.net/"), credential: new DefaultAzureCredential());

KeyVaultSecret secret = client.GetSecret("eventHubsConnectionString");
var eventHubsConnectionString = secret.Value;
secret = client.GetSecret("storageConnectionString");
var storageConnectionString = secret.Value;


var blobContainerName = "eventhubblob";

var eventHubName = "eventhubtopic1";
var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

var storageClient = new BlobContainerClient(
    storageConnectionString,
    blobContainerName);

var maximumBatchSize = 1000;

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