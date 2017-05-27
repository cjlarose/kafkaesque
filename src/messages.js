const KafkaProtocol = require('./kafka_protocol');

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
  const { header,
          requiredAcks,
          timeoutMs,
          topics } = new KafkaProtocol().read(buffer).produceRequest('value').result.value;
  const parsedTopics = topics.map(parseTopic);
  return {
    header,
    requiredAcks,
    timeoutMs,
    topics: parsedTopics,
  };
}

function writeProduceResponse(values) {
  return new KafkaProtocol().write().produceResponseV1(values).result;
}

function parseFetchRequest(buffer) {
  return new KafkaProtocol().read(buffer).fetchRequest().result;
}

function writeFetchResponse(values) {
  return new KafkaProtocol().write().fetchResponseV0(values).result;
}

function parseMetadataRequest(buffer) {
  return new KafkaProtocol().read(buffer).metadataRequest().result;
}

function writeMetadataResponse(values) {
  return new KafkaProtocol().write().metadataResponseV0(values).result;
}

function writeMessageSetElement({ offset, message }) {
  const messageBuffer = new KafkaProtocol().write().message(message).result;
  return new KafkaProtocol().write().messageSetElement({ offset, messageBuffer }).result;
}

module.exports = {
  parseProduceRequest,
  writeProduceResponse,
  parseFetchRequest,
  writeFetchResponse,
  parseMetadataRequest,
  writeMetadataResponse,
  writeMessageSetElement,
};
