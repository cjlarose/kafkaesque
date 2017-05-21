const partitionMessageSetPair = {
  read() {
    this.Int32BE('partition');
    this.Int32BE('messageSetSize');
    this.raw('messageSet', this.context.messageSetSize);
    const { partition, messageSet } = this.context;
    return { partition, messageSet };
  },
};

const topicData = {
  read() {
    this.string('name');
    this.lengthPrefixedArray('partitionMessageSetPairs', this.partitionMessageSetPair);
  },
};

const produceRequest = {
  read() {
    this.Int16BE('apiKey');
    this.Int16BE('apiVersion');
    this.Int32BE('correlationId');
    this.nullableString('clientId');
    this.Int16BE('requiredAcks');
    this.Int32BE('timeoutMs');
    this.lengthPrefixedArray('topics', this.topicData);
  },
};

module.exports = {
  partitionMessageSetPair,
  topicData,
  produceRequest,
};
