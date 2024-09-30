import { io, Manager } from "socket.io-client"; // Removed the unnecessary 'constants' import

export class createClient {
  constructor({ url, namespace = "znsocket", socket }) {
    // Correct concatenation of URL and namespace for socket connection
    if (socket) {
      this._socket = socket;
    } else if (url) {
      const path = `${url}/${namespace}`;
      this._socket = io(path);
    } else {
      // connect to the default URL with namespace
      const manager = new Manager();
      this._socket = manager.socket("/znsocket");
    }
  }
  connect() {
    return new Promise((resolve, reject) => {
      this._socket.on("connect", () => {
        resolve("Connected");
      });
    });
  }

  on(event, callback) {
    this._socket.on(event, callback);
  }

  disconnect() {
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
        resolve(data || null);
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
      this._socket.emit("hget", { name: key, key: field }, (data) => {
        resolve(data || null);
      });
    });
  }

  hSet(key, field, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "hset",
        { name: key, mapping: { [field]: value } },
        (data) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  hDel(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hdel", { name: key, key: field }, (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  hExists(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hexists", { name: key, key: field }, (data) => {
        if (data === 1) {
          resolve(true);
        }else{
          resolve(false);
        }
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

  flushAll() {
    return new Promise((resolve, reject) => {
      this._socket.emit("flushall", {}, (data) => {
        resolve("OK"); // TODO
      });
    });
  }
}
