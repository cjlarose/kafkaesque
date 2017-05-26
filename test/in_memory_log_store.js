const assert = require('assert');
const Long = require('long');
const InMemoryLogStore = require('../src/in_memory_log_store');

describe('InMemoryLogStore', () => {
  describe('#fetch', () => {
    it('returns null for empty partition', () => {
      const store = new InMemoryLogStore();

      const actualMessage = store.fetch('topic-a', 0, Long.ZERO);
      assert.strictEqual(actualMessage, null);
    });

    it('allows retrieval of first message', () => {
      const store = new InMemoryLogStore();

      const message = 'message1';
      store.append('topic-a', 0, message, 1);

      const actualMessage = store.fetch('topic-a', 0, Long.ZERO);
      assert.equal(actualMessage, message);
    });

    it('allows retrieval of second message', () => {
      const store = new InMemoryLogStore();

      store.append('topic-a', 0, '1', 1);
      store.append('topic-a', 0, '2', 1);

      const actualMessage = store.fetch('topic-a', 0, Long.fromInt(1));
      assert.equal(actualMessage, '2');
    });

    it('returns message set containing requested offset', () => {
      const store = new InMemoryLogStore();

      store.append('topic-a', 0, '1', 2);
      store.append('topic-a', 0, '2', 5);
      store.append('topic-a', 0, '3', 3);

      assert.equal(store.fetch('topic-a', 0, Long.fromInt(0)), '1');
      assert.equal(store.fetch('topic-a', 0, Long.fromInt(1)), '1');

      assert.equal(store.fetch('topic-a', 0, Long.fromInt(2)), '2');
      assert.equal(store.fetch('topic-a', 0, Long.fromInt(3)), '2');
      assert.equal(store.fetch('topic-a', 0, Long.fromInt(4)), '2');
      assert.equal(store.fetch('topic-a', 0, Long.fromInt(5)), '2');
      assert.equal(store.fetch('topic-a', 0, Long.fromInt(6)), '2');

      assert.equal(store.fetch('topic-a', 0, Long.fromInt(7)), '3');
      assert.equal(store.fetch('topic-a', 0, Long.fromInt(8)), '3');
      assert.equal(store.fetch('topic-a', 0, Long.fromInt(9)), '3');
    });

    it('returns null for an offset higher than the highest offset', () => {
      const store = new InMemoryLogStore();

      store.append('topic-a', 0, '1', 1);
      store.append('topic-a', 0, '2', 2);

      assert.strictEqual(store.fetch('topic-a', 0, Long.fromInt(3)), null);
    });
  });

  describe('#logEndOffset', () => {
    it('returns null for an empty log', () => {
      const store = new InMemoryLogStore();
      assert.strictEqual(store.logEndOffset('topic-a', 0), null);
    });

    it('returns 0 for a partition with 1 message', () => {
      const store = new InMemoryLogStore();
      store.append('topic-a', 0, 'a', 1);
      assert.deepEqual(store.logEndOffset('topic-a', 0), Long.ZERO);
    });

    it('returns 4 for a partition with 5 messages', () => {
      const store = new InMemoryLogStore();
      store.append('topic-a', 0, 'a', 5);
      assert.deepEqual(store.logEndOffset('topic-a', 0), Long.fromInt(4));
    });
  });
});
