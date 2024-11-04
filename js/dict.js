import { Client as ZnSocketClient } from "./client.js";
import {List as ZnSocketList} from "./list.js";


export class Dict {
  constructor({ client, socket, key, callbacks }) {
    this._client = client;
    this._socket = socket || (client instanceof ZnSocketClient ? client : null);
    this._key = key;
    this._callbacks = callbacks;
    this._refreshCallback = undefined;

    // Use Proxy to enable bracket notation for getting and setting
    return new Proxy(this, {
      get: (target, property, receiver) => {
        // Check if property is a method or direct property on target
        if (typeof target[property] === "function" || property in target) {
          return Reflect.get(target, property, receiver);
        }

        // For dictionary-style access, return the promise directly
        return target.get(property);
      },

      set: (target, prop, value) => {
        if (prop in target) {
          return Reflect.set(target, prop, value);
        }
        target.set(prop, value);
        return true; // Indicate success
      },
    });
  }

  async length() {
    return this._client.hLen(this._key);
  }

  async set(key, value) {
    if (this._callbacks && this._callbacks.set) {
      await this._callbacks.set(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { keys: [key] },
      });
    }

    if (value instanceof ZnSocketList) {
      value = `znsocket.List:${value._key}`;
    } else if (value instanceof Dict) {
      value = `znsocket.Dict:${value._key}`;
    }

    return this._client.hSet(this._key, key, JSON.stringify(value));
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

    // iterate over the entries and check if the value is a List or Dict
    Object.entries(dict).forEach(([key, value]) => {
      if (value instanceof ZnSocketList) {
        dict[key] = `znsocket.List:${value._key}`;
      } else if (value instanceof Dict) {
        dict[key] = `znsocket.Dict:${value._key}`;
      }
    }
    );

    const entries = Object.entries(dict).map(([key, value]) => [
      key,
      JSON.stringify(value),
    ]);
    return this._client.hMSet(this._key, Object.fromEntries(entries));
  }

  async get(key) {
    return this._client.hGet(this._key, key).then((value) => {
      if (value === null) {
        return null;
      }
      value = JSON.parse(value); // Parse the value
      if (typeof value === "string") {
        if (value.startsWith("znsocket.List:")) {
          const refKey = value.split(/:(.+)/)[1];
          return new ZnSocketList({ client: this._client,socket: this._socket , key: refKey});
        } else if (value.startsWith("znsocket.Dict:")) {
          const refKey = value.split(/:(.+)/)[1];
          return new Dict({ client: this._client, socket: this._socket , key: refKey});
        }
      }
      return value;
    });
  }

  async clear() {
    return this._client.del(this._key);
  }

  async keys() {
    return this._client.hKeys(this._key);
  }

  async values() {
    const values = await this._client.hVals(this._key);
    return values.map((x) => JSON.parse(x)); // Parse the values
  }

  async entries() { // Renamed from items to entries
    const entries = await this._client.hGetAll(this._key);
    return Object.entries(entries).map(
      ([key, value]) => [key, JSON.parse(value)]
    );
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
}
