// Python dict uses
// hget, hset, hdel, hexists, hlen, hkeys, hvals, hgetall
export class Dict {
  constructor({ client, socket, key, callbacks }) {
    this._client = client;
    this._socket = socket;
    this._key = key;
    this._callbacks = callbacks;
  }

  async len() {
    return this._client.hLen(this._key);
  }

  async setitem(key, value) {
    if (this._callbacks && this._callbacks.setitem) {
      await this._callbacks.setitem(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", { target: this._key, data: {keys: [key]} });
    }
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
    return JSON.parse(value);
  }

  async keys() {
    const keys = await this._client.hKeys(this._key);
    return keys.map((x) => JSON.parse(x));
  }

  async values() {
    const values = await this._client.hVals(this._key);
    return values.map((x) => JSON.parse(x));
  }

  async items() {
    const entries = await this._client.hGetAll(this._key);
    // Using Object.entries to return key-value pairs
    return Object.entries(entries).map(
      ([key, value]) => (JSON.parse(key), JSON.parse(value)),
    );
  }

  add_refresh_listener(callback) {
    if (this._socket) {
      this._socket.on("refresh", async ({ target, data }) => {
        if (target === this._key) {
          callback(data);
        }
      });
    }
  }
}
