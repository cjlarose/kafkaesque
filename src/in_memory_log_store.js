class InMemoryLogStore {
  constructor() {
    this.logs = {};
  }

  append(topic, partition, messageSet) {
    if (this.logs[topic] === undefined) {
      this.logs[topic] = {};
    }

    if (this.logs[topic][partition] === undefined) {
      this.logs[topic][partition] = [];
    }

    this.logs[topic][partition].push(messageSet);
  }
}

module.exports = InMemoryLogStore;
