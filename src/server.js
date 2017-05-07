const net = require('net');

const server = net.createServer();  
server.on('connection', handleConnection);

server.listen(5000, () => {  
  console.log('server listening to %j', server.address());
});


const metadataResponseBody = Buffer.from([
  0x00, 0x00, 0x00, 0x01, // broker array length = 1
  0x00, 0x00, 0x00, 0x00, // node_id
  0x00, 0x09, // broker host name length
  0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, // localhost
  0x00, 0x00, 0x13, 0x88, // port
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

function hexdump(buffer) {
  for (const pair of  buffer.entries()) {
    const [idx, byte] = pair;
    const val = byte.toString(16);
    process.stdout.write(val.length == 2 ? val : `0${val}`);

    if (idx % 16 == 15) {
      process.stdout.write(' | ');
      for (let i = idx - 15; i <= idx; i++) {
        const byteToPrint = buffer[i];
        process.stdout.write(byteToPrint >= 32 && byteToPrint <= 126 ? String.fromCharCode(byteToPrint) : '.');
      }
      process.stdout.write("\n");
    } else {
      process.stdout.write(' ');
    }
  }
  console.log();
}

function handleConnection(conn) {  
  const remoteAddress = `${conn.remoteAddress}:${conn.remotePort}`;
  console.log('new client connection from %s', remoteAddress);

  conn.on('data', onData);
  conn.once('close', onClose);
  conn.on('error', onError);

  function onData(buffer) {
    console.log('connection data from %s:', remoteAddress);
    console.log(`${buffer.length} bytes`);
    hexdump(buffer);

    const apiKey = buffer.readInt16BE(4);
    const correlationId = buffer.readInt32BE(8);
    console.log(`api_key: ${apiKey}`);
    console.log(`correlation_id: ${correlationId}`);

    if (apiKey == 3) {
      const response = Buffer.alloc(metadataResponseBody.length + 8);
      response.writeInt32BE(metadataResponseBody.length + 4, 0);
      response.writeInt32BE(correlationId, 4);
      metadataResponseBody.copy(response, 8);
      hexdump(response);
      conn.write(response);
    }
  }

  function onClose() {
    console.log('connection from %s closed', remoteAddress);
  }

  function onError(err) {
    console.log('Connection %s error: %s', remoteAddress, err.message);
  }
}
