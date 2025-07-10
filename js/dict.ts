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
	fallback?: string;
	fallbackPolicy?: "copy" | "frozen";
}

export class Dict {
	private readonly _client: ZnSocketClient;
	private readonly _socket?: ZnSocketClient;
	public readonly _key: string;
	private readonly _callbacks?: DictCallbacks;
	private readonly _refreshCallback?: (data: {
		target: string;
		data: any;
	}) => void;
	private _adapterCheckPromise: Promise<boolean> | null = null;
	private readonly _fallback?: string;
	private readonly _fallbackPolicy?: "copy" | "frozen";
	private _fallback_object?: Dict;

	constructor({
		client,
		socket,
		key,
		callbacks,
		fallback,
		fallbackPolicy,
	}: DictOptions) {
		this._client = client;
		this._socket =
			socket || (client instanceof ZnSocketClient ? client : undefined);
		this._key = `znsocket.Dict:${key}`;
		this._callbacks = callbacks;
		this._fallback = fallback;
		this._fallbackPolicy = fallbackPolicy;

		// create a fallback object if fallback is provided
		if (this._fallback) {
			this._fallback_object = new Dict({
				client: this._client,
				key: this._fallback,
				socket: this._socket,
			});
		}

		if (this._socket) {
			this._adapterCheckPromise = this._client
				.checkAdapter(this._key)
				.then((available) => {
					return available;
				});
		}

		// Use Proxy to enable bracket notation for getting and setting
		return new Proxy(this, {
			get: (target, property, receiver) => {
				// Check if property is a method or direct property on target
				if (
					typeof (target as any)[property] === "function" ||
					property in target
				) {
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
		let length = await this._client.hLen(this._key);
		if (length === 0 && this._adapterCheckPromise) {
			const adapterAvailable = await this._adapterCheckPromise;
			if (adapterAvailable) {
				length = await this._client.adapterGet(this._key, "__len__");
			}
		}

		if (length === 0 && this._fallback_object) {
			return await this._fallback_object.length();
		}
		return length;
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
		let value = await this._client.hGet(this._key, key);

		if (value === null && this._adapterCheckPromise) {
			const adapterAvailable = await this._adapterCheckPromise;
			if (adapterAvailable) {
				value = await this._client.adapterGet(this._key, "__getitem__", key);
			}
		}

		if (value === null && this._fallback_object) {
			return await this._fallback_object.get(key);
		}

		if (value === null) return null;

		value = JSON.parse(value);
		if (typeof value === "string") {
			if (value.startsWith("znsocket.List:")) {
				const refKey = value.split(/:(.+)/)[1];
				return new ZnSocketList({
					client: this._client,
					socket: this._socket,
					key: refKey,
				});
			} else if (value.startsWith("znsocket.Dict:")) {
				const refKey = value.split(/:(.+)/)[1];
				return new Dict({
					client: this._client,
					socket: this._socket,
					key: refKey,
				});
			}
		}
		return value;
	}

	async clear(): Promise<any> {
		return this._client.del(this._key);
	}

	async keys(): Promise<string[]> {
		let keys = await this._client.hKeys(this._key);
		if (keys.length === 0 && this._adapterCheckPromise) {
			const adapterAvailable = await this._adapterCheckPromise;
			if (adapterAvailable) {
				keys = await this._client.adapterGet(this._key, "keys");
			}
		}

		if (keys.length === 0 && this._fallback_object) {
			keys = await this._fallback_object.keys();
		}

		return keys;
	}

	async values(): Promise<any[]> {
		let values = await this._client.hVals(this._key);
		if (values.length === 0 && this._adapterCheckPromise) {
			const adapterAvailable = await this._adapterCheckPromise;
			if (adapterAvailable) {
				values = await this._client.adapterGet(this._key, "values");
			}
		}

		if (values.length === 0 && this._fallback_object) {
			return await this._fallback_object.values();
		}

		return values.map((x) => {
			// Parse the JSON encoded value
			let value = x;
			if (typeof x === "string") {
				try {
					value = JSON.parse(x);
				} catch {
					// If parsing fails, use the original value
					value = x;
				}
			}

			if (typeof value === "string") {
				if (value.startsWith("znsocket.List:")) {
					const refKey = value.split(/:(.+)/)[1];
					return new ZnSocketList({
						client: this._client,
						socket: this._socket,
						key: refKey,
					});
				} else if (value.startsWith("znsocket.Dict:")) {
					const refKey = value.split(/:(.+)/)[1];
					return new Dict({
						client: this._client,
						socket: this._socket,
						key: refKey,
					});
				}
			}
			return value;
		});
	}

	async entries(): Promise<[string, any][]> {
		let rawEntries: Record<string, string> | [string, any][] =
			await this._client.hGetAll(this._key);
		let processedEntries: [string, any][] = [];

		if (Object.keys(rawEntries).length === 0) {
			if (this._adapterCheckPromise) {
				const adapterAvailable = await this._adapterCheckPromise;
				if (adapterAvailable) {
					rawEntries = await this._client.adapterGet(this._key, "items");
				}
			}

			if (Object.keys(rawEntries).length === 0 && this._fallback_object) {
				return await this._fallback_object.entries();
			}
		}

		// Determine if rawEntries is an object (from hGetAll) or an array (from adapter/fallback)
		if (!Array.isArray(rawEntries)) {
			// It's an object from hGetAll, so convert to array and parse values
			processedEntries = Object.entries(rawEntries).map(([key, value]) => {
				let parsedValue = value;
				if (typeof value === "string") {
					try {
						parsedValue = JSON.parse(value);
					} catch (e) {
						// If parsing fails, it means it's not a JSON string, so use the original value
						parsedValue = value;
					}
				}
				return [key, parsedValue];
			});
		} else {
			// It's already an array from adapter or fallback, but values might still be strings
			processedEntries = rawEntries.map(([key, value]) => {
				let parsedValue = value;
				if (typeof value === "string") {
					try {
						parsedValue = JSON.parse(value);
					} catch (e) {
						// If parsing fails, it means it's not a JSON string, so use the original value
						parsedValue = value;
					}
				}
				return [key, parsedValue];
			});
		}

		// Now process the values for nested ZnSocket objects
		return processedEntries.map(([key, value]) => {
			if (typeof value === "string") {
				if (value.startsWith("znsocket.List:")) {
					const refKey = value.split(/:(.+)/)[1];
					return [
						key,
						new ZnSocketList({
							client: this._client,
							socket: this._socket,
							key: refKey,
						}),
					];
				} else if (value.startsWith("znsocket.Dict:")) {
					const refKey = value.split(/:(.+)/)[1];
					return [
						key,
						new Dict({
							client: this._client,
							socket: this._socket,
							key: refKey,
						}),
					];
				}
			}
			return [key, value];
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

	onRefresh(
		callback: (data: { keys?: string[]; indices?: number[] }) => void,
	): void {
		if (this._socket) {
			const refreshCallback = async ({
				target,
				data,
			}: {
				target: string;
				data: any;
			}) => {
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
