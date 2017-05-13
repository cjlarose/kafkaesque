const Protocol = require('bin-protocol');

const KafkaProtocol = Protocol.createProtocol();

KafkaProtocol.define('nullableString', {
  read() {
    this.Int16BE('length');
    const length = this.context.length;
    if (length === -1) {
      return null;
    } else if (length >= 0) {
      this.raw('value', length);
      return this.context.value.toString('utf8');
    }

    throw new Error('Invalid string length');
  },
});

KafkaProtocol.define('string', {
  read() {
    this.Int16BE('length');
    const length = this.context.length;
    if (length >= 0) {
      this.raw('value', length);
      return this.context.value.toString('utf8');
    }

    throw new Error('Invalid string length');
  },
  write(value) {
    if (!value) {
      throw new Error('Expected string');
    }

    this.Int16BE(value.length)
      .raw(Buffer.from(value, 'utf8'));
  },
});

KafkaProtocol.define('bytes', {
  read() {
    this.Int32BE('length');
    const length = this.context.length;
    if (length === -1) {
      return null;
    } else if (length >= 0) {
      this.raw('value', length);
      return this.context.value;
    }

    throw new Error('Invalid bytes length');
  },
});

KafkaProtocol.define('message', {
  read() {
    // TODO: Validate crc32 checksum
    this.raw('crc32', 4);
    this.Int8('magicByte');
    this.Int8('attributes');
    this.bytes('key');
    this.bytes('value');
    return this.context;
  },
});

KafkaProtocol.define('messageSetElement', {
  read() {
    this.Int64BE('offset');
    this.Int32BE('messageSize');
    this.raw('message', this.context.messageSize);
    return { offset: this.context.offset, message: this.context.message };
  },
});

KafkaProtocol.define('messageSet', {
  read() {
    this.loop('value', function readElement(end) {
      this.messageSetElement();
      if (this.offset >= this.buffer.length) {
        end();
      }
    });
    return this.context.value;
  },
});

KafkaProtocol.define('partitionMessageSetPair', {
  read() {
    this.Int32BE('partition');
    this.Int32BE('messageSetSize');
    this.raw('messageSet', this.context.messageSetSize);
    const { partition, messageSet } = this.context;
    return { partition, messageSet };
  },
});

KafkaProtocol.define('topicData', {
  read() {
    this.string('name');
    this.Int32BE('partitionMessageSetPairsLength');
    this.loop('partitionMessageSetPairs', this.partitionMessageSetPair, this.context.partitionMessageSetPairsLength);
    const { name, partitionMessageSetPairs } = this.context;
    return { name, partitionMessageSetPairs };
  },
});

KafkaProtocol.define('produceRequest', {
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
});

KafkaProtocol.define('partitionResponse', {
  write(values) {
    this.Int32BE(values.partition)
      .Int16BE(values.errorCode)
      .Int64BE(values.baseOffset);
  },
});

KafkaProtocol.define('topicResponse', {
  write(values) {
    this.string(values.topic)
      .Int32BE(values.partitionResponses.length)
      .loop(values.partitionResponses, this.partitionResponse);
  },
});

KafkaProtocol.define('produceResponseV1', {
  write(values) {
    this.Int32BE(values.correlationId)
      .Int32BE(values.topicResponses.length)
      .loop(values.topicResponses, this.topicResponse)
      .Int32BE(values.throttleTimeMs);
  },
});

function parseTopic(topic) {
  const { name, partitionMessageSetPairs } = topic;
  const parseElement = (element) => {
    const { offset, message } = element;
    const parsedMessage = new KafkaProtocol().read(message).message('value').result.value;
    return { offset, message: parsedMessage };
  };
  const parsePair = (pair) => {
    const { partition, messageSet } = pair;
    const parsedMessageSet = new KafkaProtocol().read(messageSet).messageSet('value').result.value;
    return { partition, messageSet: parsedMessageSet.map(parseElement) };
  };
  return { name, partitionMessageSetPairs: partitionMessageSetPairs.map(parsePair) };
}

function parseProduceRequest(buffer) {
  const { apiKey,
          apiVersion,
          correlationId,
          clientId,
          requiredAcks,
          timeoutMs,
          topics } = new KafkaProtocol().read(buffer).produceRequest('value').result.value;
  const parsedTopics = topics.map(parseTopic);
  return {
    apiKey,
    apiVersion,
    correlationId,
    clientId,
    requiredAcks,
    timeoutMs,
    topics: parsedTopics,
  };
}

function writeProduceResponse(values) {
  return new KafkaProtocol().write().produceResponseV1(values).result;
}

module.exports = {
  parseProduceRequest,
  writeProduceResponse,
};
