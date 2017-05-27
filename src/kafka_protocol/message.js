const message = {
  read() {
    // TODO: Validate crc32 checksum
    this.raw('crc32', 4);
    this.Int8('magicByte');
    this.Int8('attributes');
    this.bytes('key');
    this.bytes('value');
    return this.context;
  },
  write({ crc32, magicByte, attributes, key, value }) {
    this.raw(crc32)
      .Int8(magicByte)
      .Int8(attributes)
      .bytes(key)
      .bytes(value);
  },
};

module.exports = {
  message,
};
