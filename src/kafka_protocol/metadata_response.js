const brokerMetadata = {
  write(values) {
    this.Int32BE(values.nodeId)
      .string(values.host)
      .Int32BE(values.port);
  },
};

const partitionMetadata = {
  write(values) {
    this.Int16BE(values.errorCode)
      .Int32BE(values.partitionId)
      .Int32BE(values.leader)
      .Int32BE(values.replicas.length)
      .loop(values.replicas, this.Int32BE)
      .Int32BE(values.isrs.length)
      .loop(values.isrs, this.Int32BE);
  },
};

const topicMetadata = {
  write(values) {
    this.Int16BE(values.errorCode)
      .string(values.name)
      .Int32BE(values.partitionMetadata.length)
      .loop(values.partitionMetadata, this.partitionMetadata);
  },
};

const metadataResponseV0 = {
  write(values) {
    this.Int32BE(values.correlationId)
      .Int32BE(values.brokers.length)
      .loop(values.brokers, this.brokerMetadata)
      .Int32BE(values.topicMetadata.length)
      .loop(values.topicMetadata, this.topicMetadata);
  },
};

module.exports = {
  brokerMetadata,
  partitionMetadata,
  topicMetadata,
  metadataResponseV0,
};
