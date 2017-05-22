const fetchResponsePartitions = {
  write({ partitionId, errorCode, highwaterMarkOffset, messageSet }) {
    this.Int32BE(partitionId)
      .Int16BE(errorCode)
      .Int64BE(highwaterMarkOffset)
      .bytes(messageSet);
  },
};

const fetchResponseTopics = {
  write({ name, partitions }) {
    this.string(name)
      .lengthPrefixedArray(partitions, this.fetchResponsePartitions);
  },
};

const fetchResponseV0 = {
  write({ correlationId, topics }) {
    this.Int32BE(correlationId)
      .lengthPrefixedArray(topics, this.fetchResponseTopics);
  },
};

module.exports = {
  fetchResponsePartitions,
  fetchResponseTopics,
  fetchResponseV0,
};
