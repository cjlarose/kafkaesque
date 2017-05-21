const metadataRequest = {
  read() {
    this.Int16BE('apiKey');
    this.Int16BE('apiVersion');
    this.Int32BE('correlationId');
    this.nullableString('clientId');
    this.lengthPrefixedArray('topics', this.string);
  },
};

module.exports = {
  metadataRequest,
};
