const assert = require('assert');
const LengthPrefixedFrame = require('../src/length_prefixed_frame');
const Writable = require('stream').Writable;

class Collector extends Writable {
  constructor(options) {
    super(options);

    this.chunks = [];
  }

  _write(chunk, _, callback) {
    this.chunks.push(chunk);
    callback();
  }
}

describe('LengthPrefixedFrame.Decoder', () => {
  it('should yield nothing for empty input', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    decoder.write(Buffer.alloc(0));
    assert.deepEqual(collector.chunks, []);
  });

  it('should yield nothing for less than 4 bytes', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    decoder.write(Buffer.alloc(3));
    assert.deepEqual(collector.chunks, []);
  });

  it('should yield nothing for a message prefixed with 0 length', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(4);
    decoder.write(buffer);
    assert.deepEqual(collector.chunks, []);
  });

  it('should yield 1 byte for a prefixed message', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(5);
    buffer.writeInt32BE(1, 0);
    buffer[4] = 0xFF;

    decoder.write(buffer);
    assert.deepEqual(collector.chunks, [Buffer.from([0xFF])]);
  });

  it('should throw for negative lengths', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(4);
    buffer.writeInt32BE(-1, 0);

    assert.throws(() => {
      decoder.write(buffer);
    }, /Negative length not allowed/);
  });

  it('should allow length and message to arrive separately', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const lengthBuffer = Buffer.alloc(4);
    lengthBuffer.writeInt32BE(1, 0);

    const messageBuffer = Buffer.alloc(1);
    messageBuffer[0] = 0xFF;

    decoder.write(lengthBuffer);
    decoder.write(messageBuffer);
    assert.deepEqual(collector.chunks, [Buffer.from([0xFF])]);
  });

  it('should allow length to be broken into two parts', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(5);
    buffer.writeInt32BE(1, 0);
    buffer[4] = 0xFF;

    decoder.write(buffer.slice(0, 2));
    decoder.write(buffer.slice(2));
    assert.deepEqual(collector.chunks, [Buffer.from([0xFF])]);
  });

  it('should allow length to be broken into three parts', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(5);
    buffer.writeInt32BE(1, 0);
    buffer[4] = 0xFF;

    decoder.write(buffer.slice(0, 1));
    decoder.write(buffer.slice(1, 2));
    decoder.write(buffer.slice(2));
    assert.deepEqual(collector.chunks, [Buffer.from([0xFF])]);
  });

  it('should allow length to be broken into four parts', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(5);
    buffer.writeInt32BE(1, 0);
    buffer[4] = 0xFF;

    decoder.write(buffer.slice(0, 1));
    decoder.write(buffer.slice(1, 2));
    decoder.write(buffer.slice(2, 3));
    decoder.write(buffer.slice(3));
    assert.deepEqual(collector.chunks, [Buffer.from([0xFF])]);
  });

  it('should allow message to be broken up', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(8);
    buffer.writeInt32BE(4, 0);
    buffer[4] = 0xDE;
    buffer[5] = 0xAD;
    buffer[6] = 0xBE;
    buffer[7] = 0xEF;

    decoder.write(buffer.slice(0, 4));
    decoder.write(buffer.slice(4, 6));
    decoder.write(buffer.slice(6, 7));
    decoder.write(buffer.slice(7));
    assert.deepEqual(collector.chunks, [Buffer.from([0xDE, 0xAD, 0xBE, 0xEF])]);
  });

  it('should allow multiple messages in the same write', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(10);
    buffer.writeInt32BE(1, 0);
    buffer.writeInt32BE(1, 5);
    buffer[4] = 0xBE;
    buffer[9] = 0xEF;

    decoder.write(buffer);
    assert.deepEqual(collector.chunks, [Buffer.from([0xBE]), Buffer.from([0xEF])]);
  });

  it('should allow length to be split across messages', () => {
    const decoder = new LengthPrefixedFrame.Decoder();
    const collector = new Collector();
    decoder.pipe(collector);

    const buffer = Buffer.alloc(10);
    buffer.writeInt32BE(1, 0);
    buffer.writeInt32BE(1, 5);
    buffer[4] = 0xBE;
    buffer[9] = 0xEF;

    decoder.write(buffer.slice(0, 7));
    decoder.write(buffer.slice(7));
    assert.deepEqual(collector.chunks, [Buffer.from([0xBE]), Buffer.from([0xEF])]);
  });
});
