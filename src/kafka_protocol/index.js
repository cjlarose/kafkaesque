const Protocol = require('bin-protocol');
const common = require('./common');
const messageSet = require('./message_set');
const message = require('./message');
const produceRequest = require('./produce_request');
const produceResponse = require('./produce_response');
const metadataRequest = require('./metadata_request');
const metadataResponse = require('./metadata_response');

const KafkaProtocol = Protocol.createProtocol();

KafkaProtocol.define('nullableString', common.nullableString);
KafkaProtocol.define('string', common.string);
KafkaProtocol.define('bytes', common.bytes);
KafkaProtocol.define('lengthPrefixedArray', common.lengthPrefixedArray);
KafkaProtocol.define('requestHeader', common.requestHeader);

KafkaProtocol.define('messageSetElement', messageSet.messageSetElement);
KafkaProtocol.define('messageSet', messageSet.messageSet);

KafkaProtocol.define('message', message.message);

KafkaProtocol.define('partitionMessageSetPair', produceRequest.partitionMessageSetPair);
KafkaProtocol.define('topicData', produceRequest.topicData);
KafkaProtocol.define('produceRequest', produceRequest.produceRequest);

KafkaProtocol.define('partitionResponse', produceResponse.partitionResponse);
KafkaProtocol.define('topicResponse', produceResponse.topicResponse);
KafkaProtocol.define('produceResponseV1', produceResponse.produceResponseV1);

KafkaProtocol.define('metadataRequest', metadataRequest.metadataRequest);

KafkaProtocol.define('brokerMetadata', metadataResponse.brokerMetadata);
KafkaProtocol.define('partitionMetadata', metadataResponse.partitionMetadata);
KafkaProtocol.define('topicMetadata', metadataResponse.topicMetadata);
KafkaProtocol.define('metadataResponseV0', metadataResponse.metadataResponseV0);

module.exports = KafkaProtocol;
