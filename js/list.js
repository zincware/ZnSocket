import { Client as ZnSocketClient } from "./client.js";

export class List {
  constructor({ client, key, socket, callbacks }) {
    this._client = client;
    this._key = key;
    this._callbacks = callbacks;
    this._socket = socket || (client instanceof ZnSocketClient ? client : null);
    this._refreshCallback = undefined;

    return new Proxy(this, {
      get: (target, prop, receiver) => {
        // If the property is a symbol or a non-numeric property, return it directly
        if (typeof prop === "symbol" || isNaN(Number(prop))) {
          return Reflect.get(target, prop, receiver);
        }

        // Convert the property to a number if it's a numeric index
        const index = Number(prop);
        return target.get(index);
      },
      set: (target, prop, value) => {
        // If the property is a symbol or a non-numeric property, set it directly
        if (typeof prop === "symbol" || isNaN(Number(prop))) {
          return Reflect.set(target, prop, value);
        }

        // Convert the property to a number if it's a numeric index
        const index = Number(prop);
        target.set(index, value);
        return true;
      },
    });
  }

  async length() {
    return this._client.lLen(this._key);
  }

  async slice(start, end) {
    const values = await this._client.lRange(this._key, start, end);
    return values.map((value) => JSON.parse(value));
  }

  async push(value) { // Renamed from append to push
    if (this._callbacks && this._callbacks.push) {
      await this._callbacks.push(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { start: await this.length() },
      });
    }
    return this._client.rPush(this._key, JSON.stringify(value));
  }

  async set(index, value) { // Renamed from setitem to set
    if (this._callbacks && this._callbacks.set) {
      await this._callbacks.set(value);
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

  async get(index) { // Renamed from getitem to get
    const value = await this._client.lIndex(this._key, index);
    if (value === null) {
      return null;
    }
    return JSON.parse(value);
  }

  onRefresh(callback) {
    if (this._socket) {
      this._refreshCallback = async ({ target, data }) => {
        if (target === this._key) {
          callback(data);
        }
      };
      this._socket.on("refresh", this._refreshCallback);
    } else {
      throw new Error("Socket not available");
    }
  }

  offRefresh() {
    if (this._socket && this._refreshCallback) {
      this._socket.off("refresh", this._refreshCallback);
    }
  }

  [Symbol.asyncIterator]() {
    let index = 0;
    let length;

    return {
      next: async () => {
        if (length === undefined) {
          length = await this.length();
        }
        if (index >= length) {
          return { value: undefined, done: true };
        }
        const value = await this.get(index);
        index += 1;
        return { value, done: false };
      },
    };
  }
}
