import { io, Manager, Socket } from "socket.io-client";

/**
 * Options for creating a znsocket client
 */
export interface ClientOptions {
  /** The URL of the znsocket server */
  url?: string;
  /** The namespace to connect to */
  namespace?: string;
  /** An existing socket.io socket to use */
  socket?: Socket;
}

/**
 * Create a new znsocket client
 * @param options - Configuration options for the client
 * @returns A new Client instance
 */
export const createClient = ({ url, namespace = "znsocket", socket }: ClientOptions): Client => {
  return new Client({ url, namespace, socket });
};

/**
 * znsocket client for connecting to a znsocket server
 *
 * The Client class provides an interface to connect to and communicate with a znsocket server
 * using websockets. It supports Redis-like commands and provides automatic reconnection
 * capabilities.
 */
export class Client {
  private _socket: Socket;

  /**
   * Create a new znsocket client
   * @param options - Configuration options for the client
   */
  constructor({ url, namespace = "znsocket", socket }: ClientOptions) {
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

  /**
   * Connect to the znsocket server
   * @returns A promise that resolves when connected
   */
  connect(): Promise<string> {
    return new Promise((resolve, reject) => {
      this._socket.on("connect", () => {
        resolve("Connected");
      });
    });
  }

  /**
   * Register an event listener
   * @param event - The event name
   * @param callback - The callback function
   */
  on<T extends string>(event: T, callback: (...args: any[]) => void): void {
    this._socket.on(event, callback as any);
  }

  /**
   * Remove an event listener
   * @param event - The event name
   * @param callback - The callback function to remove
   */
  off<T extends string>(event: T, callback?: (...args: any[]) => void): void {
    this._socket.off(event, callback as any);
  }

  /**
   * Emit an event to the server
   * @param event - The event name
   * @param data - The data to send
   * @returns A promise that resolves with the server response
   */
  emit<T extends string>(event: T, data: any): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit(event, data, (response: any) => {
        resolve(response);
      });
    });
  }

  /**
   * Disconnect from the server
   */
  disconnect(): void {
    this._socket.close();
  }

  lLen(key: string): Promise<number> {
    return new Promise((resolve, reject) => {
      this._socket.emit("llen", [[key], {}], (data: any) => {
        // Check if there is an error or invalid response and reject if necessary
        resolve(data["data"]);
      });
    });
  }

  lIndex(key: string, index: number): Promise<string | null> {
    return new Promise((resolve, reject) => {
      this._socket.emit("lindex", [[key, index], {}], (data: any) => {
        resolve(data["data"] || null);
      });
    });
  }

  lSet(key: string, index: number, value: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "lset",
        [[key, index, value], {}],
        (data: any) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  lRem(key: string, count: number, value: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "lrem",
        [[key, count, value], {}],
        (data: any) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  lRange(key: string, start: number, end: number): Promise<string[]> {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "lrange",
        [[key, start, end], {}],
        (data: any) => {
          resolve(data["data"]);
        });
    });
  }

  rPush(key: string, value: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "rpush",
        [[key, value], {}],
        (data: any) => {
          resolve("OK"); // TODO
        });
    });
  }

  lPush(key: string, value: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit("lpush", [[key, value], {}], (data: any) => {
        resolve("OK"); // TODO
      });
    });
  }

  hGet(key: string, field: string): Promise<string | null> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hget", [[key, field], {}], (data: any) => {
        resolve(data["data"] || null);
      });
    });
  }

  hSet(key: string, field: string, value: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit(
        "hset",
        [[], { name: key, mapping: { [field]: value } }],
        (data: any) => {
          resolve("OK"); // TODO
        },
      );
    });
  }

  hMSet(key: string, mapping: Record<string, string>): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hset", [[key], { mapping: mapping }], (data: any) => {
        resolve("OK"); // TODO
      });
    });
  }

  hDel(key: string, field: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hdel", [[key, field], {}], (data: any) => {
        resolve("OK"); // TODO
      });
    });
  }

  del(key: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit("delete", [[key], {}], (data: any) => {
        resolve("OK"); // TODO
      });
    });
  }

  hExists(key: string, field: string): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hexists", [[key, field], {}], (data: any) => {
        if (data["data"] === 1) {
          resolve(true);
        } else {
          resolve(false);
        }
      });
    });
  }

  hLen(key: string): Promise<number> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hlen", [[key], {}], (data: any) => {
        resolve(data["data"]);
      });
    });
  }

  hKeys(key: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hkeys", [[key], {}], (data: any) => {
        resolve(data["data"]);
      });
    });
  }

  hVals(key: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hvals", [[key], {}], (data: any) => {
        resolve(data["data"]);
      });
    });
  }

  hGetAll(key: string): Promise<Record<string, string>> {
    return new Promise((resolve, reject) => {
      this._socket.emit("hgetall", [[key], {}], (data: any) => {
        resolve(data["data"]);
      });
    });
  }

  flushAll(): Promise<any> {
    return new Promise((resolve, reject) => {
      this._socket.emit("flushall", [[], {}], (data: any) => {
        resolve("OK"); // TODO
      });
    });
  }

  checkAdapter(key: string): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this._socket.emit("check_adapter", [[], { key: key }], (data: any) => {
        resolve(data || false);
      });
    });
  }

  adapterGet(key: string, method: string, ...args: any[]): Promise<any> {
    return new Promise((resolve, reject) => {
      let kwargs: any = { key: key, method: method };
      if (method === "__getitem__" && args.length > 0) {
        if (key.startsWith("znsocket.List:")) {
          kwargs.index = args[0];
        } else if (key.startsWith("znsocket.Dict:")) {
          kwargs.dict_key = args[0];
        }
      }
      if (method === "__contains__" && args.length > 0) {
        if (key.startsWith("znsocket.Dict:")) {
          kwargs.dict_key = args[0];
        }
      }
      if (method === "slice" && args.length >= 2) {
        kwargs.start = args[0];
        kwargs.stop = args[1];
        if (args.length > 2) {
          kwargs.step = args[2];
        }
      }
      this._socket.emit("adapter:get", [[], kwargs], (data: any) => {
        resolve(data);
      });
    });
  }
}
