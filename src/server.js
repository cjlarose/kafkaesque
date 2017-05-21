const net = require('net');
const { API_KEY } = require('./constants');
const LengthPrefixedFrame = require('./length_prefixed_frame');
const { parseProduceRequest,
        writeProduceResponse,
        parseMetadataRequest,
        writeMetadataResponse } = require('./messages');
const InMemoryLogStore = require('./in_memory_log_store');

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
      // TODO: Return only requested topics
      const request = parseMetadataRequest(requestBuffer);
      const values = {
        correlationId,
        brokers: [
          // TODO: Allow configuration of advertised brokers
          {
            nodeId: 0,
            host: 'localhost',
            port: 9092,
          },
        ],
        topicMetadata: [
          {
            errorCode: 0,
            name: 'test',
            partitionMetadata: [
              {
                errorCode: 0,
                partitionId: 0,
                leader: 0,
                replicas: [0],
                isrs: [0],
              },
            ],
          },
        ],
      };
      return writeMetadataResponse(values);
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
