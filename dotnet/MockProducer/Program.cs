// Mock producer: HTTP API on HTTP_ADDR (default :8080) that writes transfer events to Kafka.
await MockProducer.ProducerApp.Build(args).RunAsync();
