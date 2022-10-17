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

namespace MFEventHubProcessor
{

    public class MFCustomEventHubProcessor : PluggableCheckpointStoreEventProcessor<EventProcessorPartition>
    {
        // This example uses a connection string, so only the single constructor
        // was implemented; applications will need to shadow each constructor of
        // the PluggableCheckpointStoreEventProcessor that they are using.

        static long processedEventCount = 0;
        static Stopwatch sw = new Stopwatch();

        public MFCustomEventHubProcessor(
            BlobContainerClient storageClient,
            int eventBatchMaximumCount,
            string consumerGroup,
            string connectionString,
            string eventHubName,
            EventProcessorOptions clientOptions = default)
                : base(
                    new BlobCheckpointStore(storageClient),
                    eventBatchMaximumCount,
                    consumerGroup,
                    connectionString,
                    eventHubName,
                    clientOptions)
        {
        }

        protected async override Task OnProcessingEventBatchAsync(
            IEnumerable<EventData> events,
            EventProcessorPartition partition,
            CancellationToken cancellationToken)
        {
            EventData lastEvent = null;

            try
            {
                Console.WriteLine($"Received events for partition {partition.PartitionId}");

                foreach (var currentEvent in events)
                {
                    processedEventCount++;
                    //Console.WriteLine($"Event: {currentEvent.EventBody}");
                    Console.WriteLine($"Count:{processedEventCount},MsgId {currentEvent.MessageId},CorId {currentEvent.CorrelationId},Partition {partition.PartitionId} BusinessEvent {getBusinessEvent(currentEvent)} length {currentEvent.EventBody.ToArray().Length},eventBody={currentEvent.EventBody},eventsSinceLastChkpt:TODO,forceChkpt:TODO|Elapsed:{sw.ElapsedMilliseconds}|Consumed {DateTime.Now}");
                    //Console.WriteLine($"Event Count:{processedEventCount}, MessageId {currentEvent.MessageId}, CorrelationId {currentEvent.CorrelationId}, Event from partition {partition} with length {currentEvent.EventBody}, eventBody = {currentEvent.EventBody}, eventsSinceLastCheckpoint: {eventsSinceLastCheckpoint}, forceCheckpoint: {forceCheckpoint} | Elapsed: {sw.ElapsedMilliseconds} | Consume time = {DateTime.Now}");

                    ProcessEvent(currentEvent);

                    lastEvent = currentEvent;
                }

                if (lastEvent != null)
                {
                    await UpdateCheckpointAsync(
                        partition.PartitionId,
                        lastEvent.Offset,
                        lastEvent.SequenceNumber,
                        cancellationToken)
                    .ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                // It is very important that you always guard against exceptions in
                // your handler code; the processor does not have enough
                // understanding of your code to determine the correct action to take.
                // Any exceptions from your handlers go uncaught by the processor and
                // will NOT be redirected to the error handler.
                //
                // In this case, the partition processing task will fault and be restarted
                // from the last recorded checkpoint.

                Console.WriteLine($"Exception while processing events: {ex}");
            }
        }

        public string getBusinessEvent(EventData currentEvent)
        {
            string BusinessEvent = "";

            object value;
            currentEvent.Properties.TryGetValue("BusinessEvent", out value);
            if (value != null)
            {
                BusinessEvent = value.ToString();
                return BusinessEvent;
            }
            else
            {
                return "";
            }
        }

        private void ProcessEvent(EventData currentEvent)
        {

            string BusinessEvent = "";

            object value;
            currentEvent.Properties.TryGetValue("BusinessEvent", out value);
            if (value != null)
            {
                BusinessEvent = value.ToString();
            }

            //if (currentEvent.Properties.ContainsKey("BusinessEvent")) { var BusinessEvent2 = currentEvent.Properties["BusinessEvent"]; }

            //    var x = currentEvent.Properties["BusinessEvent"];
        }

        protected override Task OnProcessingErrorAsync(
            Exception exception,
            EventProcessorPartition partition,
            string operationDescription,
            CancellationToken cancellationToken)
        {
            try
            {
                if (partition != null)
                {
                    Console.Error.WriteLine(
                        $"Exception on partition {partition.PartitionId} while " +
                        $"performing {operationDescription}: {exception}");
                }
                else
                {
                    Console.Error.WriteLine(
                        $"Exception while performing {operationDescription}: {exception}");
                }
            }
            catch (Exception ex)
            {
                // It is very important that you always guard against exceptions
                // in your handler code; the processor does not have enough
                // understanding of your code to determine the correct action to
                // take.  Any exceptions from your handlers go uncaught by the
                // processor and will NOT be handled in any way.
                //
                // In this case, unhandled exceptions will not impact the processor
                // operation but will go unobserved, hiding potential application problems.

                Console.WriteLine($"Exception while processing events: {ex}");
            }

            return Task.CompletedTask;
        }

        protected override Task OnInitializingPartitionAsync(
            EventProcessorPartition partition,
            CancellationToken cancellationToken)
        {
            try
            {
                Console.WriteLine($"Initializing partition {partition.PartitionId}");
            }
            catch (Exception ex)
            {
                // It is very important that you always guard against exceptions in
                // your handler code; the processor does not have enough
                // understanding of your code to determine the correct action to take.
                // Any exceptions from your handlers go uncaught by the processor and
                // will NOT be redirected to the error handler.
                //
                // In this case, the partition processing task will fault and the
                // partition will be initialized again.

                Console.WriteLine($"Exception while initializing a partition: {ex}");
            }
            sw.Start();
            return Task.CompletedTask;
        }

        protected override Task OnPartitionProcessingStoppedAsync(
            EventProcessorPartition partition,
            ProcessingStoppedReason reason,
            CancellationToken cancellationToken)
        {
            try
            {
                Console.WriteLine(
                    $"No longer processing partition {partition.PartitionId} " +
                    $"because {reason}");
                sw.Stop();
            }
            catch (Exception ex)
            {
                // It is very important that you always guard against exceptions in
                // your handler code; the processor does not have enough
                // understanding of your code to determine the correct action to take.
                // Any exceptions from your handlers go uncaught by the processor and
                // will NOT be redirected to the error handler.
                //
                // In this case, unhandled exceptions will not impact the processor
                // operation but will go unobserved, hiding potential application problems.

                Console.WriteLine($"Exception while stopping processing for a partition: {ex}");
            }

            return Task.CompletedTask;
        }
    }

}