const net = require('net');
const { API_KEY } = require('./constants');
const LengthPrefixedFrame = require('./length_prefixed_frame');
const { parseProduceRequest, writeProduceResponse } = require('./messages');
const InMemoryLogStore = require('./in_memory_log_store');

const metadataResponseBody = Buffer.from([
  0x00, 0x00, 0x00, 0x01, // broker array length = 1
  0x00, 0x00, 0x00, 0x00, // node_id
  0x00, 0x09, // broker host name length
  0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, // localhost
  0x00, 0x00, 0x23, 0x84, // port
  0x00, 0x00, 0x00, 0x01, // topic metadata array length
  0x00, 0x00, // topic error code
  0x00, 0x04, // topic name length
  0x74, 0x65, 0x73, 0x74, // "test"
  0x00, 0x00, 0x00, 0x01, // partition metadata array length
  0x00, 0x00, // partition error code
  0x00, 0x00, 0x00, 0x00, // partition id
  0x00, 0x00, 0x00, 0x00, // leader
  0x00, 0x00, 0x00, 0x01, // replica length
  0x00, 0x00, 0x00, 0x00, // replica 0
  0x00, 0x00, 0x00, 0x01, // isr length
  0x00, 0x00, 0x00, 0x00, // isr 0
]);

const store = new InMemoryLogStore();

function handleRequest(requestBuffer) {
  const apiKey = requestBuffer.readInt16BE(0);
  const correlationId = requestBuffer.readInt32BE(4);
  console.log(`api_key: ${apiKey}`);
  console.log(`correlation_id: ${correlationId}`);

  switch (apiKey) {
    case API_KEY.PRODUCE: {
      const message = parseProduceRequest(requestBuffer);

      const topicResponses = message.topics.map((topic) => {
        const partitionResponses = topic.partitionMessageSetPairs.map((partitionMessageSetPair) => {
          const { partition, messageSet } = partitionMessageSetPair;
          store.append(topic.name, partition, messageSet);

          return { partition, errorCode: 0, baseOffset: 0 };
        });

        return { topic: topic.name, partitionResponses };
      });

      const responseValues = { correlationId, topicResponses, throttleTimeMs: 30000 };
      const responseBuffer = writeProduceResponse(responseValues);

      return responseBuffer;
    }
    case API_KEY.METADATA: {
      const response = Buffer.alloc(metadataResponseBody.length + 4);
      response.writeInt32BE(correlationId, 0);
      metadataResponseBody.copy(response, 4);
      return response;
    }
    default:
      return null;
  }
}

function handleConnection(socket) {
  const remoteAddress = `${socket.remoteAddress}:${socket.remotePort}`;
  console.log('new client connection from %s', remoteAddress);

  function onEnd() {
    console.log('received FIN from connection from %s', remoteAddress);
  }

  function onClose() {
    console.log('connection from %s closed', remoteAddress);
  }

  function onError(err) {
    console.log('Connection %s error: %s', remoteAddress, err.message);
  }

  socket.on('end', onEnd);
  socket.once('close', onClose);
  socket.on('error', onError);

  const clientFrames = new LengthPrefixedFrame.Decoder();
  socket.pipe(clientFrames);

  const serverFrames = new LengthPrefixedFrame.Encoder();
  serverFrames.pipe(socket);

  function handleClientFrame(buffer) {
    const responseFrame = handleRequest(buffer);
    if (responseFrame !== null) {
      serverFrames.write(responseFrame);
    }
  }

  clientFrames.on('data', handleClientFrame);
}

const server = net.createServer();
server.on('connection', handleConnection);

server.listen(9092, () => {
  console.log('server listening to %j', server.address());
});
