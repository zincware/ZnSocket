// Python list uses
// llen, lindex, lset, lrem, rpush, lpush, linsert, lrange, rpush

export class List {
  constructor({ client, key }) {
    this._client = client;
    this._key = key;
  }

  async len() {
    return this._client.lLen(this._key);
  }

  async append(value) {
    return this._client.rPush(this._key, JSON.stringify(value));
  }

  async setitem(index, value) {
    return this._client.lSet(this._key, index, JSON.stringify(value));
  }

  async getitem(index) {
    const value = await this._client.lIndex(this._key, index);
    if (value === null) {
      return null;
    }
    return JSON.parse(value);
  }

  [Symbol.asyncIterator]() {
    let index = 0;
    return {
      next: async () => {
        const value = await this.getitem(index);
        index += 1;
        return { value, done: value === null };
      },
    };
  }
}
