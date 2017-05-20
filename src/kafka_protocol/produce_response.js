const partitionResponse = {
  write(values) {
    this.Int32BE(values.partition)
      .Int16BE(values.errorCode)
      .Int64BE(values.baseOffset);
  },
};

const topicResponse = {
  write(values) {
    this.string(values.topic)
      .Int32BE(values.partitionResponses.length)
      .loop(values.partitionResponses, this.partitionResponse);
  },
};

const produceResponseV1 = {
  write(values) {
    this.Int32BE(values.correlationId)
      .Int32BE(values.topicResponses.length)
      .loop(values.topicResponses, this.topicResponse)
      .Int32BE(values.throttleTimeMs);
  },
};

module.exports = {
  partitionResponse,
  topicResponse,
  produceResponseV1,
};

