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
    this.Int32BE('partitionMessageSetPairsLength');
    this.loop('partitionMessageSetPairs', this.partitionMessageSetPair, this.context.partitionMessageSetPairsLength);
    const { name, partitionMessageSetPairs } = this.context;
    return { name, partitionMessageSetPairs };
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
    this.Int32BE('topicsLength');
    this.loop('topics', this.topicData, this.context.topicsLength);
    const { apiKey, apiVersion, correlationId,
      clientId, requiredAcks, timeoutMs, topics } = this.context;
    return { apiKey, apiVersion, correlationId, clientId, requiredAcks, timeoutMs, topics };
  },
};

module.exports = {
  partitionMessageSetPair,
  topicData,
  produceRequest,
};
