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
  private _refreshCallback?: (data: { target: string; data: any }) => void;
  private _adapterAvailable: boolean = false;
  private _adapterCheckPromise: Promise<boolean> | null = null;

  constructor({ client, key, socket, callbacks }: ListOptions) {
    this._client = client;
    this._key = `znsocket.List:${key}`;
    this._callbacks = callbacks;
    this._socket = socket || (client instanceof ZnSocketClient ? client : undefined);

    if (this._socket) {
      this._adapterCheckPromise = this._client.checkAdapter(this._key).then(available => {
        this._adapterAvailable = available;
        return available;
      });
    }

    return new Proxy(this, {
      get: (target, prop, receiver) => {
        if (typeof prop === "symbol" || isNaN(Number(prop))) {
          return Reflect.get(target, prop, receiver);
        }
        const index = Number(prop);
        return target.get(index);
      },
      set: (target, prop, value) => {
        if (typeof prop === "symbol" || isNaN(Number(prop))) {
          return Reflect.set(target, prop, value);
        }
        const index = Number(prop);
        target.set(index, value);
        return true;
      },
    });
  }

  async length(): Promise<number> {
    const length = await this._client.lLen(this._key);
    if (length === 0 && this._adapterCheckPromise) {
      const adapterAvailable = await this._adapterCheckPromise;
      if (adapterAvailable) {
        return await this._client.adapterGet(this._key, "__len__");
      }
    }
    return length;
  }

  async slice(start: number, end: number, step: number = 1): Promise<any[]> {
    // Check if adapter is available and use it for slicing
    if (this._adapterCheckPromise) {
      const adapterAvailable = await this._adapterCheckPromise;
      if (adapterAvailable) {
        const length = await this.length();
        // Convert negative indices to positive
        if (start < 0) start = Math.max(0, length + start);
        if (end < 0) end = Math.max(0, length + end);
        
        const adapterValues = await this._client.adapterGet(this._key, "slice", start, end, step);
        return adapterValues.map((value: any) => {
          value = JSON.parse(value);

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
        });
      }
    }
    
    // Fallback to Redis lRange for non-adapter lists
    // Note: Redis lRange is inclusive on both ends, but JavaScript slice is exclusive on end
    // So we adjust end-1 to match JavaScript slice semantics
    const values = await this._client.lRange(this._key, start, end - 1);
    return values.map((value) => JSON.parse(value));
  }

  async push(value: any): Promise<any> {
    if (this._callbacks?.push) {
      await this._callbacks.push(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { start: await this.length() },
      });
    }
    if (value instanceof List || value instanceof ZnSocketDict) {
      value = value._key;
    }
    return this._client.rPush(this._key, JSON.stringify(value));
  }

  async set(index: number, value: any): Promise<any> {
    if (this._callbacks?.set) {
      await this._callbacks.set(value);
    }
    if (this._socket) {
      this._socket.emit("refresh", {
        target: this._key,
        data: { indices: [index] },
      });
    }
    if (value instanceof List || value instanceof ZnSocketDict) {
      value = value._key;
    }
    return this._client.lSet(this._key, index, JSON.stringify(value));
  }

  async clear(): Promise<any> {
    if (this._callbacks?.clear) {
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

  async get(index: number): Promise<any | null> {
    let value = await this._client.lIndex(this._key, index);

    if (value === null && this._adapterCheckPromise) {
      const adapterAvailable = await this._adapterCheckPromise;
      if (adapterAvailable) {
        value = await this._client.adapterGet(this._key, "__getitem__", index);
        if (value === null) return null;

        value = JSON.parse(value);
        
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
    }

    if (value === null) return null;

    value = JSON.parse(value);

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
    if (!this._socket) throw new Error("Socket not available");

    this._refreshCallback = ({ target, data }) => {
      if (target === this._key) {
        callback(data);
      }
    };

    this._socket.on("refresh", this._refreshCallback);
  }

  offRefresh(): void {
    if (this._socket && this._refreshCallback) {
      this._socket.off("refresh", this._refreshCallback);
    }
  }

  async toObject(): Promise<any[]> {
    const result: any[] = [];
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
        const value = await this.get(index++);
        return { value, done: false };
      },
    };
  }
}
