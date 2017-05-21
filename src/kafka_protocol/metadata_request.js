const metadataRequest = {
  read() {
    this.requestHeader('header')
      .lengthPrefixedArray('topics', this.string);
  },
};

module.exports = {
  metadataRequest,
};
