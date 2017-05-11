const net = require('net');
const { API_KEY } = require('./constants');

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

function handleRequest(requestBuffer) {
  const apiKey = requestBuffer.readInt16BE(0);
  const correlationId = requestBuffer.readInt32BE(4);
  console.log(`api_key: ${apiKey}`);
  console.log(`correlation_id: ${correlationId}`);

  if (apiKey === API_KEY.METADATA) {
    const response = Buffer.alloc(metadataResponseBody.length + 8);
    response.writeInt32BE(metadataResponseBody.length + 4, 0);
    response.writeInt32BE(correlationId, 4);
    metadataResponseBody.copy(response, 8);
    return response;
  }

  return null;
}

function handleConnection(socket) {
  const remoteAddress = `${socket.remoteAddress}:${socket.remotePort}`;
  console.log('new client connection from %s', remoteAddress);

  function onData(buffer) {
    console.log('connection data from %s:', remoteAddress);
    console.log(`${buffer.length} bytes`);

    const requestBuffer = buffer.slice(4);
    const responseBuffer = handleRequest(requestBuffer);
    if (responseBuffer !== null) {
      socket.write(responseBuffer);
    }
  }

  function onEnd() {
    console.log('received FIN from connection from %s', remoteAddress);
  }

  function onClose() {
    console.log('connection from %s closed', remoteAddress);
  }

  function onError(err) {
    console.log('Connection %s error: %s', remoteAddress, err.message);
  }

  socket.on('data', onData);
  socket.on('end', onEnd);
  socket.once('close', onClose);
  socket.on('error', onError);
}

const server = net.createServer();
server.on('connection', handleConnection);

server.listen(9092, () => {
  console.log('server listening to %j', server.address());
});
