const fetchRequestPartition = {
  read() {
    this.Int32BE('partitionId')
      .Int64BE('offset')
      .Int32BE('maxBytes');
  },
};

const fetchRequestTopic = {
  read() {
    this.string('name')
      .lengthPrefixedArray('partitions', this.fetchRequestPartition);
  },
};

const fetchRequest = {
  read() {
    this.requestHeader('header')
      .Int32BE('replicaId')
      .Int32BE('maxWaitTimeMs')
      .Int32BE('minBytes')
      .lengthPrefixedArray('topics', this.fetchRequestTopic);
  },
};

module.exports = {
  fetchRequestPartition,
  fetchRequestTopic,
  fetchRequest,
};
