const metadataRequest = {
  read() {
    this.Int16BE('apiKey');
    this.Int16BE('apiVersion');
    this.Int32BE('correlationId');
    this.nullableString('clientId');
    this.Int32BE('topicsLength');

    let topics = [];
    if (this.context.topicsLength !== 0) {
      this.loop('topics', this.string, this.context.topicsLength);
      topics = this.context.topics;
    }

    const { apiKey, apiVersion, correlationId, clientId } = this.context;
    return { apiKey, apiVersion, correlationId, clientId, topics };
  },
};

module.exports = {
  metadataRequest,
};
