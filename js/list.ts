import { Client as ZnSocketClient } from "./client.js";
import { Dict as ZnSocketDict, Dict } from "./dict.js";

export interface ListCallbacks {
  push?: (value: any) => Promise<any>;
  set?: (value: any) => Promise<any>;
  clear?: () => Promise<any>;
}

export interface ListOptions {
  client: ZnSocketClient;
  key: string;
  socket?: ZnSocketClient;
  callbacks?: ListCallbacks;
}

export class List {
  private readonly _client: ZnSocketClient;
  public readonly _key: string;
  private readonly _callbacks?: ListCallbacks;
  private readonly _socket?: ZnSocketClient;
  private readonly _refreshCallback?: (data: { target: string; data: any }) => void;

  constructor({ client, key, socket, callbacks }: ListOptions) {
    this._client = client;
    this._key = `znsocket.List:${key}`;
    this._callbacks = callbacks;
    this._socket = socket || (client instanceof ZnSocketClient ? client : undefined);

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

  async length(): Promise<number> {
    return this._client.lLen(this._key);
  }

  async slice(start: number, end: number): Promise<any[]> {
    const values = await this._client.lRange(this._key, start, end);
    return values.map((value) => JSON.parse(value));
  }

  async push(value: any): Promise<any> { // Renamed from append to push
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

  async set(index: number, value: any): Promise<any> { // Renamed from setitem to set
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

  async clear(): Promise<any> {
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

  async get(index: number): Promise<any | null> { // Renamed from getitem to get
    let value = await this._client.lIndex(this._key, index);
    if (value === null) {
      return null;
    }
    value = JSON.parse(value); // Parse the value
    if (typeof value === "string") {
      if (value.startsWith("znsocket.List:")) {
        const refKey = value.split(/:(.+)/)[1];
        return new List({ client: this._client, socket: this._socket, key: refKey });
      } else if (value.startsWith("znsocket.Dict:")) {
        const refKey = value.split(/:(.+)/)[1];
        return new ZnSocketDict({ client: this._client, socket: this._socket, key: refKey });
      }
    }
    return value;
  }

  onRefresh(callback: (data: { start?: number; indices?: number[] }) => void): void {
    if (this._socket) {
      const refreshCallback = async ({ target, data }: { target: string; data: any }) => {
        if (target === this._key) {
          callback(data);
        }
      };
      this._socket.on("refresh", refreshCallback);
    } else {
      throw new Error("Socket not available");
    }
  }

  offRefresh(): void {
    if (this._socket && this._refreshCallback) {
      this._socket.off("refresh", this._refreshCallback);
    }
  }

  async toObject(): Promise<any[]> {
    const result = [];
    const len = await this.length();
    for (let i = 0; i < len; i++) {
      const value = await this.get(i);
      if (value instanceof Dict || value instanceof List) {
        result.push(await value.toObject());
      } else {
        result.push(value);
      }
    }
    return result;
  }

  [Symbol.asyncIterator](): AsyncIterator<any | undefined> {
    let index = 0;
    let length: number | undefined;

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
