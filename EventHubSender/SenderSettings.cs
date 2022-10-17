namespace EventHubSender
{
    public class SenderSettings
    {
        public string? AkvUri { get; set; }
        public int MessageSendDelay { get; set; }

        public string? EventHubsConnectionStringConfigSettingName { get; set; }

        public string? EventHubName { get; set; }

        public int BatchSize { get; set; }
        
        //Sleep time between batches in milliseconds
        public int InterBatchSleepTime { get; set; }
        public int NumberOfMessagesToSend { get; set; }
        public string? PartitionKey { get; set; }

        public string? EventHubsConnectionStringOverride { get; set; }

    }
}

