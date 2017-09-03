# Kafkaesque

Kafkaesque is a log store that is API-compatible with [Apache Kafka][kafka-home]. It trades read and write throughput for reduced operational complexity by storing its data in a PostgreSQL database.

When Kafka receives a write request, it writes new logs to a file, but does not commit those writes to disk immediately. This is very fast, but compromises durability: if all of the brokers on a topic die unexpectedly, logs can be lost. Kafka mitigates this risk by allowing cluster administrators to configure a replication factor for each topic. The replication factor specifies the number of nodes in the cluster that must acknowledge a write before it is reported as successful to the log producer.

Kafkaesque leans on durability instead of replication for fault tolerance. For every write operation, Kafkaesque inserts new logs into a PostgreSQL table. PostgreSQL, in turn, commits those logs to disk before Kafkaesque reports success to the log producer. This means Kafkaesque can tolerate unexpected loss of power, for example, without losing data and without the burden of operating a complex cluster of brokers. This comes at the cost, of course, of availibility and throughput.

Kafkaesque is suitable for applications that have use cases that are addressed by the style of development Kafka promotes, but do not have the scalability demands for which Kafka is optimized or the operational budget it demands. Because Kafkaesque uses PostgreSQL for its data backend, it is especially well-suited for use with RDBMS-as-a-service products like AWS's RDS.

Because the Kafkaesque server is API-compatible with Kafka, you can use any of the existing [Kafka client libraries][clients] with Kafkaesque to produce and consume logs. This means that it is possible to use Kafkaesqueue when developing an application and to use Kafka in staging and production environments.

[kafka-home]: http://kafka.apache.org/
[clients]: https://cwiki.apache.org/confluence/display/KAFKA/Clients
