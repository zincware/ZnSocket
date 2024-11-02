import { Socket } from "socket.io-client";
import { ZnSocketClient } from "./client"; // Assuming client.d.ts is already defined

interface ClientOptions {
  url?: string;
  namespace?: string;
  socket?: Socket;
}

export class Client {
  constructor(options: ClientOptions);
  connect(): Promise<string>;
  on<T extends string>(event: T, callback: (data: any) => void): void;
  off<T extends string>(event: T, callback?: (data: any) => void): void;
  emit<T extends string>(event: T, data: any): Promise<any>;
  disconnect(): void;

  lLen(key: string): Promise<number>;
  lIndex(key: string, index: number): Promise<string | null>;
  lSet(key: string, index: number, value: string): Promise<any>;
  lRem(key: string, count: number, value: string): Promise<any>;
  lRange(key: string, start: number, end: number): Promise<string[]>;
  rPush(key: string, value: string): Promise<any>;
  lPush(key: string, value: string): Promise<any>;
  hGet(key: string, field: string): Promise<string | null>;
  hSet(key: string, field: string, value: string): Promise<any>;
  hMSet(key: string, mapping: Record<string, string>): Promise<any>;
  hDel(key: string, field: string): Promise<any>;
  del(key: string): Promise<any>;
  hExists(key: string, field: string): Promise<boolean>;
  hLen(key: string): Promise<number>;
  hKeys(key: string): Promise<string[]>;
  hVals(key: string): Promise<string[]>;
  hGetAll(key: string): Promise<Record<string, string>>;
  flushAll(): Promise<any>;
}

export const createClient: (options: ClientOptions) => Client;


interface ListCallbacks {
  push?: (value: any) => Promise<any>;
  set?: (value: any) => Promise<any>;
  clear?: () => Promise<any>;
}

export class List {
  private readonly _client: ZnSocketClient;
  private readonly _key: string;
  private readonly _callbacks?: ListCallbacks;
  private readonly _socket?: ZnSocketClient; // Can be null if not provided
  private readonly _refreshCallback?: (data: { target: string; data: any }) => void;

  constructor(options: { client: ZnSocketClient; key: string; socket?: ZnSocketClient; callbacks?: ListCallbacks });

  length(): Promise<number>;
  slice(start: number, end: number): Promise<any[]>;
  push(value: any): Promise<any>;
  set(index: number, value: any): Promise<any>;
  clear(): Promise<any>;
  get(index: number): Promise<any | null>;

  onRefresh(callback: (data: { start?: number; indices?: number[] }) => void): void;
  offRefresh(): void;

  [Symbol.asyncIterator](): AsyncIterator<any | undefined>;
}


interface DictCallbacks {
  set?: (value: any) => Promise<any>;
  update?: (value: Record<string, any>) => Promise<any>;
}

export class Dict {
  private readonly _client: ZnSocketClient;
  private readonly _socket?: ZnSocketClient; // Can be null if not provided
  private readonly _key: string;
  private readonly _callbacks?: DictCallbacks;
  private readonly _refreshCallback?: (data: { target: string; data: any }) => void;

  constructor(options: { client: ZnSocketClient; socket?: ZnSocketClient; key: string; callbacks?: DictCallbacks });

  length(): Promise<number>;
  set(key: string, value: any): Promise<any>;
  update(dict: Record<string, any>): Promise<any>;
  get(key: string): Promise<any | null>;
  clear(): Promise<any>;
  keys(): Promise<string[]>;
  values(): Promise<any[]>;
  entries(): Promise<[string, any][]>; // Renamed from items to entries

  onRefresh(callback: (data: { keys?: string[]; indices?: number[] }) => void): void;
  offRefresh(): void;
}
