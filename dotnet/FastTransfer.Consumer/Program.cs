// FAST consumer: reads the FAST topics with KAFKA_GROUP and serves /metrics + /healthz on METRICS_ADDR (default :2114).
await FastTransfer.Consumer.ConsumerApp.Build(args).RunAsync();
