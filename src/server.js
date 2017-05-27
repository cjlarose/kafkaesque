const net = require('net');
const { API_KEY } = require('./constants');
const LengthPrefixedFrame = require('./length_prefixed_frame');
const { parseProduceRequest,
        writeProduceResponse,
        parseFetchRequest,
        writeFetchResponse,
        parseMetadataRequest,
        writeMetadataResponse,
        writeMessageSetElement } = require('./messages');
const InMemoryLogStore = require('./in_memory_log_store');

const store = new InMemoryLogStore();

function handleRequest(requestBuffer) {
  const apiKey = requestBuffer.readInt16BE(0);
  console.log(`api_key: ${apiKey}`);

  switch (apiKey) {
    case API_KEY.PRODUCE: {
      const request = parseProduceRequest(requestBuffer);
      const correlationId = request.header.correlationId;

      const topicResponses = request.topics.map((topic) => {
        const partitionResponses = topic.partitionMessageSetPairs.map((partitionMessageSetPair) => {
          const { partition, messageSet } = partitionMessageSetPair;
          messageSet.forEach((message) => {
            const encodedMessage = writeMessageSetElement(message);
            store.append(topic.name, partition, encodedMessage, 1);
          });

          return { partition, errorCode: 0, baseOffset: 0 };
        });

        return { topic: topic.name, partitionResponses };
      });

      console.log(JSON.stringify(store.logs, null, 4));

      const responseValues = { correlationId, topicResponses, throttleTimeMs: 30000 };
      const responseBuffer = writeProduceResponse(responseValues);

      return responseBuffer;
    }
    case API_KEY.FETCH: {
      // TODO: respect maxWaitTimeMs
      // TODO: respect minBytes
      // TODO: respect maxBytes

      const request = parseFetchRequest(requestBuffer);
      const correlationId = request.header.correlationId;
      console.log(JSON.stringify(request, null, 4));

      const topicResponses = request.topics.map((topic) => {
        const partitionResponses = topic.partitions.map(({ partitionId, offset }) => {
          console.log(topic.name, partitionId, offset);
          // TODO: Respond with error if topic/partition does not exist
          const message = store.fetch(topic.name, partitionId, offset);
          const highwaterMarkOffset = store.logEndOffset(topic.name, partitionId);
          const errorCode = message === null ? 1 : 0;
          return {
            partitionId,
            highwaterMarkOffset,
            messageSet: [message],
            errorCode,
          };
        });

        return { name: topic.name, partitions: partitionResponses };
      });

      console.log(JSON.stringify(topicResponses, null, 4));
      const responseValues = { correlationId, topics: topicResponses };
      const responseBuffer = writeFetchResponse(responseValues);

      return responseBuffer;
    }
    case API_KEY.METADATA: {
      // TODO: Return only requested topics
      const request = parseMetadataRequest(requestBuffer);
      const correlationId = request.header.correlationId;
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
            name: 'topic-a',
            partitionMetadata: [
              {
                errorCode: 0,
                partitionId: 0,
                leader: 0,
                replicas: [0],
                isrs: [0],
              },
              {
                errorCode: 0,
                partitionId: 1,
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
