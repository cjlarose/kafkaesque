const net = require('net');

const server = net.createServer();  
server.on('connection', handleConnection);

server.listen(5000, () => {  
  console.log('server listening to %j', server.address());
});

function handleConnection(conn) {  
  const remoteAddress = `${conn.remoteAddress}:${conn.remotePort}`;
  console.log('new client connection from %s', remoteAddress);

  conn.on('data', onData);
  conn.once('close', onClose);
  conn.on('error', onError);

  function onData(buffer) {
    console.log('connection data from %s:', remoteAddress);
    for (const pair of  buffer.entries()) {
      const [idx, byte] = pair;
      const val = byte.toString(16);
      process.stdout.write(val.length == 2 ? val : `0${val}`);
      process.stdout.write(' ');
    }
    console.log();
  }

  function onClose() {
    console.log('connection from %s closed', remoteAddress);
  }

  function onError(err) {
    console.log('Connection %s error: %s', remoteAddress, err.message);
  }
}
