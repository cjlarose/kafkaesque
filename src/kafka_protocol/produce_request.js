const partitionMessageSetPair = {
  read() {
    this.Int32BE('partition')
      .bytes('messageSet');
  },
};

const topicData = {
  read() {
    this.string('name')
      .lengthPrefixedArray('partitionMessageSetPairs', this.partitionMessageSetPair);
  },
};

const produceRequest = {
  read() {
    this.Int16BE('apiKey')
      .Int16BE('apiVersion')
      .Int32BE('correlationId')
      .nullableString('clientId')
      .Int16BE('requiredAcks')
      .Int32BE('timeoutMs')
      .lengthPrefixedArray('topics', this.topicData);
  },
};

module.exports = {
  partitionMessageSetPair,
  topicData,
  produceRequest,
};
