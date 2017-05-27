const messageSetElement = {
  read() {
    this.Int64BE('offset');
    this.Int32BE('messageSize');
    this.raw('message', this.context.messageSize);
    return { offset: this.context.offset, message: this.context.message };
  },
  write({ offset, messageBuffer }) {
    this.Int64BE(offset);
    this.bytes(messageBuffer);
  },
};

const messageSet = {
  read() {
    this.loop('value', function readElement(end) {
      this.messageSetElement();
      if (this.offset >= this.buffer.length) {
        end();
      }
    });
    return this.context.value;
  },
};

module.exports = {
  messageSetElement,
  messageSet,
};
