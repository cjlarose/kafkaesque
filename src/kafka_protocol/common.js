const nullableString = {
  read() {
    this.Int16BE('length');
    const length = this.context.length;
    if (length === -1) {
      return null;
    } else if (length >= 0) {
      this.raw('value', length);
      return this.context.value.toString('utf8');
    }

    throw new Error('Invalid string length');
  },
};

const string = {
  read() {
    this.Int16BE('length');
    const length = this.context.length;
    if (length >= 0) {
      this.raw('value', length);
      return this.context.value.toString('utf8');
    }

    throw new Error('Invalid string length');
  },
  write(value) {
    if (!value) {
      throw new Error('Expected string');
    }

    this.Int16BE(value.length)
      .raw(Buffer.from(value, 'utf8'));
  },
};

const bytes = {
  read() {
    this.Int32BE('length');
    const length = this.context.length;
    if (length === -1) {
      return null;
    } else if (length >= 0) {
      this.raw('value', length);
      return this.context.value;
    }

    throw new Error('Invalid bytes length');
  },
};

module.exports = {
  nullableString,
  string,
  bytes,
};
