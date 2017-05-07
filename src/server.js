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
    console.log(`${buffer.length} bytes`);
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

  function onClose() {
    console.log('connection from %s closed', remoteAddress);
  }

  function onError(err) {
    console.log('Connection %s error: %s', remoteAddress, err.message);
  }
}
