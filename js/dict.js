// Python dict uses
// hget, hset, hdel, hexists, hlen, hkeys, hvals, hgetall
export class Dict {
  constructor(client, key) {
    this._client = client;
    this._key = key;
  }

  async len() {
    return this._client.hLen(this._key);
  }

  async setitem(key, value) {
    return this._client.hSet(
      this._key,
      JSON.stringify(key),
      JSON.stringify(value),
    );
  }

  async getitem(key) {
    const value = await this._client.hGet(this._key, JSON.stringify(key));
    if (value === null) {
      return null;
    }
    return JSON.stringify(value);
  }

  async keys() {
    return this._client.hKeys(this._key);
  }

  async values() {
    // JSON
    return this._client.hVals(this._key);
  }

  async items() {
    // JSON
    return this._client.hGetAll(this._key);
  }
}
