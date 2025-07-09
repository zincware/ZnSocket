import { Client as ZnSocketClient } from "./client.js";
import { Dict as ZnSocketDict } from "./dict.js";

export class List {
  constructor({ client, key, socket, callbacks }) {
    this._client = client;
    this._key = `znsocket.List:${key}`;
    this._callbacks = callbacks;
    this._socket = socket || (client instanceof ZnSocketClient ? client : null);
    this._refreshCallback = undefined;
    this._adapterAvailable = false;
    this._adapterCheckPromise = null;
    
    // Check for adapter availability
    if (this._socket) {
      this._adapterCheckPromise = this._client.checkAdapter(this._key).then(available => {
        this._adapterAvailable = available;
        return available;
      });
    }

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
    const length = await this._client.lLen(this._key);
    if (length === 0 && this._adapterCheckPromise) {
      const adapterAvailable = await this._adapterCheckPromise;
      if (adapterAvailable) {
        return await this._client.adapterGet(this._key, "__len__");
      }
    }
    return length;
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
    if (value instanceof List) {
      value = value._key;
    } else if (value instanceof ZnSocketDict) {
      value = value._key;
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
    if (value instanceof List) {
      value = value._key;
    } else if (value instanceof ZnSocketDict) {
      value = value._key;
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
    let value = await this._client.lIndex(this._key, index);
    if (value === null && this._adapterCheckPromise) {
      const adapterAvailable = await this._adapterCheckPromise;
      if (adapterAvailable) {
        value = await this._client.adapterGet(this._key, "__getitem__", index);
        if (value === null) {
          return null;
        }
        // Adapter values are JSON-encoded, need to parse them
        if (typeof value === "string") {
          try {
            value = JSON.parse(value);
          } catch (e) {
            // If parsing fails, return as-is
          }
        }
        
        // Handle references to other objects
        if (typeof value === "string") {
          if (value.startsWith("znsocket.List:")) {
            const refKey = value.split(/:(.+)/)[1];
            return new List({ client: this._client,socket: this._socket , key: refKey});
          } else if (value.startsWith("znsocket.Dict:")) {
            const refKey = value.split(/:(.+)/)[1];
            return new ZnSocketDict({ client: this._client, socket: this._socket , key: refKey});
          }
        }
        return value;
      }
    }
    if (value === null) {
      return null;
    }
    value = JSON.parse(value); // Parse the value
    if (typeof value === "string") {
      if (value.startsWith("znsocket.List:")) {
        const refKey = value.split(/:(.+)/)[1];
        return new List({ client: this._client,socket: this._socket , key: refKey});
      } else if (value.startsWith("znsocket.Dict:")) {
        const refKey = value.split(/:(.+)/)[1];
        return new ZnSocketDict({ client: this._client, socket: this._socket , key: refKey});
      }
    }
    return value;
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
