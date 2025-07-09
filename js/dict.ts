import { Client as ZnSocketClient } from "./client.js";
import { List as ZnSocketList } from "./list.js";

export interface DictCallbacks {
  set?: (value: any) => Promise<any>;
  update?: (value: Record<string, any>) => Promise<any>;
}

export interface DictOptions {
  client: ZnSocketClient;
  socket?: ZnSocketClient;
  key: string;
  callbacks?: DictCallbacks;
}

export class Dict {
  private readonly _client: ZnSocketClient;
  private readonly _socket?: ZnSocketClient;
  public readonly _key: string;
  private readonly _callbacks?: DictCallbacks;
  private readonly _refreshCallback?: (data: { target: string; data: any }) => void;

  constructor({ client, socket, key, callbacks }: DictOptions) {
    this._client = client;
    this._socket = socket || (client instanceof ZnSocketClient ? client : undefined);
    this._key = `znsocket.Dict:${key}`;
    this._callbacks = callbacks;

    // Use Proxy to enable bracket notation for getting and setting
    return new Proxy(this, {
      get: (target, property, receiver) => {
        // Check if property is a method or direct property on target
        if (typeof (target as any)[property] === "function" || property in target) {
          return Reflect.get(target, property, receiver);
        }

        // For dictionary-style access, return the promise directly
        return target.get(property as string);
      },

      set: (target, prop, value) => {
        if (prop in target) {
          return Reflect.set(target, prop, value);
        }
        target.set(prop as string, value);
        return true; // Indicate success
      },
    });
  }

  async length(): Promise<number> {
    return this._client.hLen(this._key);
  }

  async set(key: string, value: any): Promise<any> {
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
      value = value._key;
    } else if (value instanceof Dict) {
      value = value._key;
    }

    return this._client.hSet(this._key, key, JSON.stringify(value));
  }

  async update(dict: Record<string, any>): Promise<any> {
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
        dict[key] = value._key;
      } else if (value instanceof Dict) {
        dict[key] = value._key;
      }
    });

    const entries = Object.entries(dict).map(([key, value]) => [
      key,
      JSON.stringify(value),
    ]);
    return this._client.hMSet(this._key, Object.fromEntries(entries));
  }

  async get(key: string): Promise<any | null> {
    return this._client.hGet(this._key, key).then((value) => {
      if (value === null) {
        return null;
      }
      value = JSON.parse(value); // Parse the value
      if (typeof value === "string") {
        if (value.startsWith("znsocket.List:")) {
          const refKey = value.split(/:(.+)/)[1];
          return new ZnSocketList({ client: this._client, socket: this._socket, key: refKey });
        } else if (value.startsWith("znsocket.Dict:")) {
          const refKey = value.split(/:(.+)/)[1];
          return new Dict({ client: this._client, socket: this._socket, key: refKey });
        }
      }
      return value;
    });
  }

  async clear(): Promise<any> {
    return this._client.del(this._key);
  }

  async keys(): Promise<string[]> {
    return this._client.hKeys(this._key);
  }

  async values(): Promise<any[]> {
    const values = await this._client.hVals(this._key);
    return values.map((x) => {
      const value = JSON.parse(x);
      if (typeof value === "string") {
        if (value.startsWith("znsocket.List:")) {
          const refKey = value.split(/:(.+)/)[1];
          return new ZnSocketList({ client: this._client, socket: this._socket, key: refKey });
        } else if (value.startsWith("znsocket.Dict:")) {
          const refKey = value.split(/:(.+)/)[1];
          return new Dict({ client: this._client, socket: this._socket, key: refKey });
        }
      }
      return value;
    });
  }

  async entries(): Promise<[string, any][]> {
    const entries = await this._client.hGetAll(this._key);
    return Object.entries(entries).map(([key, value]) => {
      const parsedValue = JSON.parse(value);

      if (typeof parsedValue === "string") {
        if (parsedValue.startsWith("znsocket.List:")) {
          const refKey = parsedValue.split(/:(.+)/)[1];
          return [key, new ZnSocketList({ client: this._client, socket: this._socket, key: refKey })];
        } else if (parsedValue.startsWith("znsocket.Dict:")) {
          const refKey = parsedValue.split(/:(.+)/)[1];
          return [key, new Dict({ client: this._client, socket: this._socket, key: refKey })];
        }
      }

      return [key, parsedValue];
    });
  }

  async toObject(): Promise<Record<string, any>> {
    const entries = await this.entries();
    // go through all and if one of them is a Dict or List, call toObject on it
    const obj: Record<string, any> = {};
    for (const [key, value] of entries) {
      if (value instanceof Dict || value instanceof ZnSocketList) {
        obj[key] = await value.toObject();
      } else {
        obj[key] = value;
      }
    }
    return obj;
  }

  onRefresh(callback: (data: { keys?: string[]; indices?: number[] }) => void): void {
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
}
