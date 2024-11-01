// Python list uses
// llen, lindex, lset, lrem, rpush, lpush, linsert, lrange, rpush
// see also https://gist.github.com/stelf/d97ab0156461ffc5fbc65be54b936abf
import { Client as ZnSocketClient } from "./client.js";
export class List {
  constructor({ client, key, socket, callbacks }) {
    this._client = client;
    this._key = key;
    this._callbacks = callbacks;
    this._socket = socket || (client instanceof ZnSocketClient ? client : null);
    this._refresh_callback = undefined;

    return new Proxy(this, {
      get: (target, prop) => {
        // If the property is a symbol or a non-numeric property, return it directly
        if (typeof prop === "symbol" || isNaN(Number(prop))) {
          return target[prop];
        }

        // Convert the property to a number if it's a numeric index
        const index = Number(prop);
        return target.getitem(index);
      },
      set: (target, prop, value) => {
        // If the property is a symbol or a non-numeric property, set it directly
        if (typeof prop === "symbol" || isNaN(Number(prop))) {
          target[prop] = value;
          return true;
        }

        // Convert the property to a number if it's a numeric index
        const index = Number(prop);
        target.setitem(index, value);
        return true;
      }
    });
  }

  async len() {
    return this._client.lLen(this._key);
  }

  async slice(start, end) {
    const values = await this._client.lRange(this._key, start, end);
    return values.map((value) => JSON.parse(value));
  }

  async append(value) {
    if (this._callbacks && this._callbacks.append) {
      await this._callbacks.append(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { start: await this.len() },
      });
    }
    return this._client.rPush(this._key, JSON.stringify(value));
  }

  async setitem(index, value) {
    if (this._callbacks && this._callbacks.setitem) {
      await this._callbacks.setitem(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { indices: [index] },
      });
    }
    return this._client.lSet(this._key, index, JSON.stringify(value));
  }

  async clear() {
    if (this._callbacks && this._callbacks.clear) {
      await this._callbacks.clear();
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { start: 0 },
      });
    }
    return this._client.del(this._key);
  }

  async getitem(index) {
    const value = await this._client.lIndex(this._key, index);
    if (value === null) {
      return null;
    }
    return JSON.parse(value);
  }

  onRefresh(callback) {
    if (this._socket) {
      this._refresh_callback = async ({ target, data }) => {
        if (target === this._key) {
          callback(data);
        }
      };
      this._socket.on("refresh", this._refresh_callback);
    } else {
      throw new Error("Socket not available");
    }
  }

  offRefresh() {
    if (this._socket && this._refresh_callback) {
      this._socket.off("refresh", this._refresh_callback);
    }
  }

  [Symbol.asyncIterator]() {
    let index = 0;
    let length;

    return {
      next: async () => {
        if (length === undefined) {
          length = await this.len();
        }
        if (index >= length) {
          return { value: undefined, done: true };
        }
        const value = await this.getitem(index);
        index += 1;
        return { value, done: false };
      },
    };
  }
}
