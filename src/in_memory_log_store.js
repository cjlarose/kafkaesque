const Long = require('long');

class InMemoryLogStore {
  constructor() {
    this.logs = {};
  }

  append(topic, partitionId, message, messageCount) {
    if (this.logs[topic] === undefined) {
      this.logs[topic] = {};
    }

    if (this.logs[topic][partitionId] === undefined) {
      this.logs[topic][partitionId] = { offset: Long.NEG_ONE, messages: [] };
    }

    const partition = this.logs[topic][partitionId];
    partition.messages.push({ messageCount, message });
    partition.offset = partition.offset.add(messageCount);
  }

  logEndOffset(topic, partitionId) {
    if (this.logs[topic] === undefined || this.logs[topic][partitionId] === undefined) {
      return null;
    }

    return this.logs[topic][partitionId].offset;
  }

  // returns the message set that contains the message at given offset
  fetch(topic, partitionId, offset) {
    if (this.logs[topic] === undefined || this.logs[topic][partitionId] === undefined) {
      return null;
    }

    const partition = this.logs[topic][partitionId].messages;

    let messageSetIndex = 0;
    let fetchOffset = Long.ZERO;

    while (messageSetIndex < partition.length) {
      const log = partition[messageSetIndex];

      fetchOffset = fetchOffset.add(log.messageCount);

      if (fetchOffset.gt(offset)) {
        return log.message;
      }

      messageSetIndex += 1;
    }

    // offset too high
    return null;
  }
}

module.exports = InMemoryLogStore;
