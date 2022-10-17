namespace EventHubReceiver
{
    public class ReceiverSettings
    {
        public string? AkvUri { get; set; }

        public string? EventHubsConnectionStringConfigSettingName { get; set; }

        public string? EventHubName { get; set; }

        public int MaximumBatchSize { get; set; }
        
        public string? ConsumerGroup{ get; set; }

        public string? StorageAccountConnectionStringConfigSettingName { get; set; }

        public string? StorageAccountBlobContainerNameConfigSettingName { get; set; }

        public string? EventHubsConnectionStringOverride { get; set; }
        public string? StorageAccountConnectionStringOverride { get; set; }

    }
}

