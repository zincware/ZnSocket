import { io, Manager } from "socket.io-client"; // Removed the unnecessary 'constants' import

export const createClient = ({ url, namespace = "znsocket", socket }) => {
  return new Client({ url, namespace, socket });
};

export class Client {
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
  off(event, callback) {
    this._socket.off(event, callback);
  }

  emit(event, data) {
    this._socket.emit(event, data);
  }

  disconnect() {
    this._socket.close();
  }

  lLen(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("llen", [[key], {}], (data) => {
        // Check if there is an error or invalid response and reject if necessary
        resolve(data["data"]);
      });
    });
  }

  lIndex(key, index) {
    return new Promise((resolve, reject) => {
      this._socket.emit("lindex", [[key, index],{}], (data) => {
        resolve(data["data"] || null);
      });
    });
  }

  lSet(key, index, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "lset",
        [[key, index, value], {}],
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
        [[key, count, value], {}],
        (data) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  lRange(key, start, end) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "lrange",
        [[key, start, end - 1], {}],
        (data) => {
        resolve(data["data"]);
      });
    });
  }

  rPush(key, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "rpush",
        [[key, value], {}],
        (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  lPush(key, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit("lpush", [[key, value], {}], (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  hGet(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hget", [[key, field], {}], (data) => {
        resolve(data["data"] || null);
      });
    });
  }

  hSet(key, field, value) {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "hset",
        [[], { name: key, mapping: { [field]: value } }],
        (data) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  hMSet(key, mapping) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hset", [[key], {mapping: mapping}], (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  hDel(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hdel", [[key, field], {}], (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  del(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("delete", [[key],{}], (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  hExists(key, field) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hexists", [[key, field], {}], (data) => {
        if (data["data"] === 1) {
          resolve(true);
        } else {
          resolve(false);
        }
      });
    });
  }

  hLen(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hlen", [[key],{}], (data) => {
        resolve(data["data"]);
      });
    });
  }

  hKeys(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hkeys", [[key],{}], (data) => {
        resolve(data["data"]);
      });
    });
  }

  hVals(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hvals", [[key], {}], (data) => {
        resolve(data["data"]);
      });
    });
  }

  hGetAll(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("hgetall", [[key], {}], (data) => {
        resolve(data["data"]);
      });
    });
  }

  flushAll() {
    return new Promise((resolve, reject) => {
      this._socket.emit("flushall", [[], {}], (data) => {
        resolve("OK"); // TODO
      });
    });
  }

  // Adapter protocol methods
  checkAdapter(key) {
    return new Promise((resolve, reject) => {
      this._socket.emit("check_adapter", [[], { key: key }], (data) => {
        resolve(data || false);
      });
    });
  }

  adapterGet(key, method, ...args) {
    return new Promise((resolve, reject) => {
      let kwargs = { key: key, method: method };
      if (method === "__getitem__" && args.length > 0) {
        kwargs.index = args[0];
      }
      this._socket.emit("adapter:get", [[], kwargs], (data) => {
        resolve(data);
      });
    });
  }
}
