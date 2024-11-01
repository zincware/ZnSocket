// Python dict uses
// hget, hset, hdel, hexists, hlen, hkeys, hvals, hgetall
import { Client as ZnSocketClient } from "./client.js";

export class Dict {
  constructor({ client, socket, key, callbacks }) {
    this._client = client;
    this._socket = socket || (client instanceof ZnSocketClient ? client : null);
    // this._socket = socket;
    this._key = key;
    this._callbacks = callbacks;
    this._refresh_callback = undefined;

    // Use Proxy to enable bracket notation for getting and setting
    return new Proxy(this, {
      // get that does nothing
      get: (target, property, receiver) => {
        // Check if property is a method or direct property on target
        if (typeof target[property] === "function" || property in target) {
          return Reflect.get(target, property, receiver).bind(target);
        }
    
        // For dictionary-style access, return the promise directly
        return target.getitem(property);
      },

      set: async (target, prop, value) => {
        if (prop in target) {
          target[prop] = value;
          return true; // Indicate success
        }
        await target.setitem(prop, value); // Await the async setitem call
        return true; // Indicate success
      }
    //   ownKeys: async (target) => {
    //     // For Object.keys
    //     const keys = await target.keys();
    //     return keys;
    //   },
    //   getOwnPropertyDescriptor: () => ({
    //     enumerable: true,
    //     configurable: true
    //   })
    });
  }

  async len() {
    return this._client.hLen(this._key);
  }

  async setitem(key, value) {
    if (this._callbacks && this._callbacks.setitem) {
      await this._callbacks.setitem(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { keys: [key] },
      });
    }
    return this._client.hSet(
      this._key,
      key,
      JSON.stringify(value),
    );
  }

  async update(dict) {
    if (this._callbacks && this._callbacks.update) {
      await this._callbacks.update(dict);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { keys: Object.keys(dict) },
      });
    }
    // TODO: we do not properly stringify the keys
    const entries = Object.entries(dict).map(([key, value]) => [key,
      JSON.stringify(value),
    ]);
    return this._client.hMSet(this._key, Object.fromEntries(entries));
  }

  async getitem(key) {
    return this._client.hGet(this._key, key).then((value) => {
      if (value === null) {
        return null;
      }
      return JSON.parse(value);
    });
  }

  async clear() {
    return this._client.del(this._key);
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
      ([key, value]) => [JSON.parse(key), JSON.parse(value)]
    );
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
}
