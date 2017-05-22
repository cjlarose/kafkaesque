const Transform = require('stream').Transform;

const LENGTH_PREFIX_SIZE = 4;

class BufferList {
  constructor() {
    // TODO: Replace buffers Array with Deque for O(1) shift and push
    // TODO: Prevent buffer copies by passing around buffer lists that simulate contiguous buffers
    this.buffers = [];
    this.length = 0;
  }

  append(buffer) {
    this.buffers.push(buffer);
    this.length += buffer.length;
  }

  unshiftBytes(length) {
    if (length > this.length) {
      throw new Error('Not enough bytes in buffers');
    }

    const outputBuffer = Buffer.allocUnsafe(length);
    let offset = 0;

    while (offset < length) {
      const buffer = this.buffers[0];
      const bytesToCopy = Math.min(length - offset, buffer.length);
      buffer.copy(outputBuffer, offset, 0, bytesToCopy);
      offset += bytesToCopy;

      if (bytesToCopy === buffer.length) {
        this.buffers.shift();
      } else {
        this.buffers[0] = buffer.slice(bytesToCopy);
      }

      this.length -= bytesToCopy;
    }

    return outputBuffer;
  }
}

class Decoder extends Transform {
  constructor(options) {
    super(options);

    this.expectedLength = null;
    this.bufferList = new BufferList();
  }

  extractMessageLength() {
    if (this.bufferList.length < LENGTH_PREFIX_SIZE) {
      return null;
    }

    const lengthBuffer = this.bufferList.unshiftBytes(LENGTH_PREFIX_SIZE);
    const length = lengthBuffer.readInt32BE(0);

    if (length < 0) {
      throw new Error('Negative length not allowed');
    }

    return length;
  }

  constructOutputBuffer() {
    if (this.expectedLength === null) {
      this.expectedLength = this.extractMessageLength();
    }

    if (this.expectedLength === null || this.bufferList.length < this.expectedLength) {
      return null;
    }

    const outputBuffer = this.bufferList.unshiftBytes(this.expectedLength);
    this.expectedLength = null;

    return outputBuffer;
  }

  _transform(chunk, _, callback) {
    this.bufferList.append(chunk);

    let outputBuffer;

    while (outputBuffer = this.constructOutputBuffer()) {
      this.push(outputBuffer);
    }

    callback();
  }
}

class Encoder extends Transform {
  _transform(chunk, _, callback) {
    const lengthBuffer = Buffer.alloc(4);
    lengthBuffer.writeIntBE(chunk.length, 0, LENGTH_PREFIX_SIZE);
    this.push(lengthBuffer);
    this.push(chunk);
    callback();
  }
}

module.exports = {
  Decoder,
  Encoder,
};
