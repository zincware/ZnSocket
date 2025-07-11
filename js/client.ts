import { io, Manager, type Socket } from "socket.io-client";
import { v4 as uuidv4 } from "uuid";

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
	/** Maximum message size in bytes before chunking (default: 80MB) */
	maxMessageSizeBytes?: number;
}

/**
 * Create a new znsocket client
 * @param options - Configuration options for the client
 * @returns A new Client instance
 */
export const createClient = ({
	url,
	namespace = "znsocket",
	socket,
}: ClientOptions): Client => {
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
	private _maxMessageSizeBytes: number;

	/**
	 * Create a new znsocket client
	 * @param options - Configuration options for the client
	 */
	constructor({ url, namespace = "znsocket", socket, maxMessageSizeBytes = 80 * 1024 * 1024 }: ClientOptions) {
		this._maxMessageSizeBytes = maxMessageSizeBytes;
		
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
	 * Serialize message data to string
	 * @param data - The data to serialize
	 * @returns Serialized data as string
	 */
	private _serializeMessage(data: any): string {
		return JSON.stringify(data);
	}

	/**
	 * Split message string into chunks
	 * @param message - The message string to split
	 * @param maxChunkSize - Maximum size per chunk
	 * @returns Array of chunk strings
	 */
	private _splitMessageString(message: string, maxChunkSize: number): string[] {
		const chunks: string[] = [];
		for (let i = 0; i < message.length; i += maxChunkSize) {
			chunks.push(message.substring(i, i + maxChunkSize));
		}
		return chunks;
	}

	/**
	 * Send a chunked message to the server
	 * @param event - The event name
	 * @param data - The data to send
	 * @returns Promise that resolves with server response
	 */
	private async _emitChunked(event: string, data: any): Promise<any> {
		const messageString = this._serializeMessage(data);
		const chunkSize = this._maxMessageSizeBytes - 200; // Reserve space for chunk metadata
		const chunks = this._splitMessageString(messageString, chunkSize);
		const chunkId = uuidv4();
		
		console.debug(`Splitting message into ${chunks.length} chunks with ID ${chunkId}`);
		
		// Send all chunks
		for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
			const chunkMetadata = {
				chunk_id: chunkId,
				chunk_index: chunkIndex,
				total_chunks: chunks.length,
				event: event,
				data: chunks[chunkIndex]
			};
			
			const response = await new Promise((resolve, reject) => {
				this._socket.emit("chunked_message", chunkMetadata, (response: any) => {
					if (response?.error) {
						reject(new Error(`Chunk ${chunkIndex} failed: ${response.error}`));
					} else {
						resolve(response);
					}
				});
			});
		}
		
		// Get final response
		return new Promise((resolve, reject) => {
			this._socket.emit("get_chunked_result", { chunk_id: chunkId }, (response: any) => {
				if (response?.error) {
					reject(new Error(`Chunked message failed: ${response.error}`));
				} else {
					resolve(response);
				}
			});
		});
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
	 * Automatically handles chunking for large messages that exceed the size limit.
	 * @param event - The event name
	 * @param data - The data to send
	 * @returns A promise that resolves with the server response
	 */
	emit<T extends string>(event: T, data: any): Promise<any> {
		// Check if message needs chunking
		const messageString = this._serializeMessage(data);
		const messageSize = new TextEncoder().encode(messageString).length;
		
		if (messageSize > this._maxMessageSizeBytes) {
			// Use chunked transmission
			console.debug(`Message size (${messageSize.toLocaleString()} bytes) exceeds limit (${this._maxMessageSizeBytes.toLocaleString()} bytes). Using chunked transmission.`);
			return this._emitChunked(event, data);
		} else {
			// Use normal transmission
			return new Promise((resolve, reject) => {
				this._socket.emit(event, data, (response: any) => {
					resolve(response);
				});
			});
		}
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
				resolve(data.data);
			});
		});
	}

	lIndex(key: string, index: number): Promise<string | null> {
		return new Promise((resolve, reject) => {
			this._socket.emit("lindex", [[key, index], {}], (data: any) => {
				resolve(data.data || null);
			});
		});
	}

	lSet(key: string, index: number, value: string): Promise<any> {
		return new Promise((resolve, reject) => {
			this._socket.emit("lset", [[key, index, value], {}], (data: any) => {
				resolve("OK"); // TODO
			});
		});
	}

	lRem(key: string, count: number, value: string): Promise<any> {
		return new Promise((resolve, reject) => {
			this._socket.emit("lrem", [[key, count, value], {}], (data: any) => {
				resolve("OK"); // TODO
			});
		});
	}

	lRange(key: string, start: number, end: number): Promise<string[]> {
		return new Promise((resolve, reject) => {
			this._socket.emit("lrange", [[key, start, end], {}], (data: any) => {
				resolve(data.data);
			});
		});
	}

	rPush(key: string, value: string): Promise<any> {
		return new Promise((resolve, reject) => {
			this._socket.emit("rpush", [[key, value], {}], (data: any) => {
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
				resolve(data.data || null);
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
				if (data.data === 1) {
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
				resolve(data.data);
			});
		});
	}

	hKeys(key: string): Promise<string[]> {
		return new Promise((resolve, reject) => {
			this._socket.emit("hkeys", [[key], {}], (data: any) => {
				resolve(data.data);
			});
		});
	}

	hVals(key: string): Promise<string[]> {
		return new Promise((resolve, reject) => {
			this._socket.emit("hvals", [[key], {}], (data: any) => {
				resolve(data.data);
			});
		});
	}

	hGetAll(key: string): Promise<Record<string, string>> {
		return new Promise((resolve, reject) => {
			this._socket.emit("hgetall", [[key], {}], (data: any) => {
				resolve(data.data);
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
			const kwargs: any = { key: key, method: method };
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

	/**
	 * Create a pipeline for batching commands
	 * @param maxCommandsPerCall - Maximum commands per pipeline call
	 * @returns A new Pipeline instance
	 */
	pipeline(maxCommandsPerCall: number = 1000000): Pipeline {
		return new Pipeline(this, maxCommandsPerCall);
	}
}

/**
 * Pipeline class for batching multiple commands
 */
export class Pipeline {
	private _client: Client;
	private _commands: Array<[string, any]> = [];
	private _maxCommandsPerCall: number;

	constructor(client: Client, maxCommandsPerCall: number = 1000000) {
		this._client = client;
		this._maxCommandsPerCall = maxCommandsPerCall;
	}

	/**
	 * Add a command to the pipeline
	 * @param command - Command name
	 * @param data - Command data
	 * @returns This pipeline instance for chaining
	 */
	private _addCommand(command: string, data: any): Pipeline {
		this._commands.push([command, data]);
		return this;
	}

	/**
	 * Execute all commands in the pipeline
	 * @returns Promise that resolves with array of results
	 */
	async execute(): Promise<any[]> {
		if (this._commands.length === 0) {
			return [];
		}

		const results: any[] = [];
		const commands = [...this._commands];
		
		// Process commands in batches
		for (let i = 0; i < commands.length; i += this._maxCommandsPerCall) {
			const batch = commands.slice(i, i + this._maxCommandsPerCall);
			const message = batch.map(([command, data]) => [command, data]);
			
			const batchResults = await this._client.emit("pipeline", { message });
			if (batchResults?.data) {
				results.push(...batchResults.data);
			}
		}
		
		return results;
	}

	// Add common Redis-like methods to the pipeline
	hset(key: string, field: string, value: any): Pipeline {
		return this._addCommand("hset", [[], { name: key, mapping: { [field]: value } }]);
	}

	hget(key: string, field: string): Pipeline {
		return this._addCommand("hget", [[key, field], {}]);
	}

	set(key: string, value: any): Pipeline {
		return this._addCommand("set", [[key, value], {}]);
	}

	get(key: string): Pipeline {
		return this._addCommand("get", [[key], {}]);
	}

	lpush(key: string, value: any): Pipeline {
		return this._addCommand("lpush", [[key, value], {}]);
	}

	rpush(key: string, value: any): Pipeline {
		return this._addCommand("rpush", [[key, value], {}]);
	}

	llen(key: string): Pipeline {
		return this._addCommand("llen", [[key], {}]);
	}

	lindex(key: string, index: number): Pipeline {
		return this._addCommand("lindex", [[key, index], {}]);
	}
}
