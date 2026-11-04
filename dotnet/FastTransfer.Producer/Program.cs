// FAST producer: HTTP API on HTTP_ADDR (default :8080) that writes FAST events to Kafka.
await FastTransfer.Producer.ProducerApp.Build(args).RunAsync();
