import { io } from "socket.io-client"; // Removed the unnecessary 'constants' import

export class Client {
  constructor(url, namespace = "znsocket") {
    // Correct concatenation of URL and namespace for socket connection
    const path = `${url}/${namespace}`;
    // throw new Error(url); // logs ws://127.0.0.1:10063/znsocket
    this._socket = io(path);
  }

  close() {
    this._socket.close();
  }

  lLen(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("llen", { name: key }, (data) => {
        // Check if there is an error or invalid response and reject if necessary
        resolve(data);
      });
    });
  }

  lIndex(key, index) {
    return new Promise((resolve, reject) => {
      this._socket.emit("lindex", { name: key, index: index }, (data) => {
        resolve(data);
      });
    });
  }

  lSet(key, index, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "lset",
        { name: key, index: index, value: value },
        (data) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  lRem(key, count, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "lrem",
        { name: key, count: count, value: value },
        (data) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  rPush(key, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit("rpush", { name: key, value: value }, (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  lPush(key, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit("lpush", { name: key, value: value }, (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  hGet(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hget", { name: key, field: field }, (data) => {
        resolve(data);
      });
    });
  }

  hSet(key, field, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "hset",
        { name: key, field: field, value: value },
        (data) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  hDel(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hdel", { name: key, field: field }, (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  hExists(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hexists", { name: key, field: field }, (data) => {
        resolve(data);
      });
    });
  }

  hLen(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hlen", { name: key }, (data) => {
        resolve(data);
      });
    });
  }

  hKeys(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hkeys", { name: key }, (data) => {
        resolve(data);
      });
    });
  }

  hVals(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hvals", { name: key }, (data) => {
        resolve(data);
      });
    });
  }

  hGetAll(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hgetall", { name: key }, (data) => {
        resolve(data);
      });
    });
  }

  flushall() {
    return new Promise((resolve, reject) => {
      this._socket.emit("flushall", {}, (data) => {
        resolve("OK"); // TODO
      });
    });
  }
}


// Python list uses
// llen, lindex, lset, lrem, rpush, lpush, linsert, lrange, rpush

// Python dict uses
// hget, hset, hdel, hexists, hlen, hkeys, hvals, hgetall

export class List {
  constructor(client, key) {
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
