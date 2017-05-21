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
      .lengthPrefixedArray(values.replicas, this.Int32BE)
      .lengthPrefixedArray(values.isrs, this.Int32BE);
  },
};

const topicMetadata = {
  write(values) {
    this.Int16BE(values.errorCode)
      .string(values.name)
      .lengthPrefixedArray(values.partitionMetadata, this.partitionMetadata);
  },
};

const metadataResponseV0 = {
  write(values) {
    this.Int32BE(values.correlationId)
      .lengthPrefixedArray(values.brokers, this.brokerMetadata)
      .lengthPrefixedArray(values.topicMetadata, this.topicMetadata);
  },
};

module.exports = {
  brokerMetadata,
  partitionMetadata,
  topicMetadata,
  metadataResponseV0,
};
