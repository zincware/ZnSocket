import type { Client as ZnSocketClient } from "./client.js";
import { List as ZnSocketList } from "./list.js";

export interface SegmentsOptions {
	client: ZnSocketClient;
	key: string;
	socket?: ZnSocketClient;
	callbacks?: any;
}

export class Segments {
	private readonly _client: ZnSocketClient;
	private readonly _key: string;

	constructor({ client, key, socket, callbacks }: SegmentsOptions) {
		this._client = client;
		this._key = `znsocket.Segments:${key}`;

		return new Proxy(this, {
			get: (target, prop, receiver) => {
				// If the property is a symbol or a non-numeric property, return it directly
				if (typeof prop === "symbol" || Number.isNaN(Number(prop))) {
					return Reflect.get(target, prop, receiver);
				}

				// Convert the property to a number if it's a numeric index
				const index = Number(prop);
				return target.get(index);
			},
			// set: (target, prop, value) => {
			//   // If the property is a symbol or a non-numeric property, set it directly
			//   if (typeof prop === "symbol" || isNaN(Number(prop))) {
			//     return Reflect.set(target, prop, value);
			//   }

			//   // Convert the property to a number if it's a numeric index
			//   const index = Number(prop);
			//   target.set(index, value);
			//   return true;
			// },
		});
	}

	async length(): Promise<number> {
		const segments = await this._client.hGetAll(this._key);
		let length = 0;
		Object.values(segments).forEach((segment: string) => {
			// segment is [start, end, target]
			const [start, end] = JSON.parse(segment);
			length += end - start;
		});
		return length;
	}

	// async slice(start, end) {
	//   const values = await this._client.lRange(this._key, start, end);
	//   return values.map((value) => JSON.parse(value));
	// }

	// async push(value) { // Renamed from append to push
	//   if (this._callbacks && this._callbacks.push) {
	//     await this._callbacks.push(value);
	//   }
	//   if (this._socket) {
	//     this._socket.emit("refresh", {
	//       target: this._key,
	//       data: { start: await this.length() },
	//     });
	//   }
	//   if (value instanceof List) {
	//     value = value._key;
	//   } else if (value instanceof ZnSocketDict) {
	//     value = value._key;
	//   }
	//   return this._client.rPush(this._key, JSON.stringify(value));
	// }

	// async set(index, value) { // Renamed from setitem to set
	//   if (this._callbacks && this._callbacks.set) {
	//     await this._callbacks.set(value);
	//   }
	//   if (this._socket) {
	//     this._socket.emit("refresh", {
	//       target: this._key,
	//       data: { indices: [index] },
	//     });
	//   }
	//   if (value instanceof List) {
	//     value = value._key;
	//   } else if (value instanceof ZnSocketDict) {
	//     value = value._key;
	//   }
	//   return this._client.lSet(this._key, index, JSON.stringify(value));
	// }

	// async clear() {
	//   if (this._callbacks && this._callbacks.clear) {
	//     await this._callbacks.clear();
	//   }
	//   if (this._socket) {
	//     this._socket.emit("refresh", {
	//       target: this._key,
	//       data: { start: 0 },
	//     });
	//   }
	//   return this._client.del(this._key);
	// }

	async get(index: number): Promise<any | null> {
		const segments = await this._client.hGetAll(this._key);
		const items = [];
		let size = 0;

		for (const segment of Object.values(segments)) {
			const [start, end, target] = JSON.parse(segment);
			const listKey = target.split(/:(.+)/)[1];
			const lst = new ZnSocketList({ client: this._client, key: listKey });

			if (size <= index && index < size + (end - start)) {
				const offset = index + start - size;
				const item = await lst.get(offset);
				items.push(item);
			}
			size += end - start;
		}
		return items.length > 0 ? items[0] : null;
	}

	// onRefresh(callback) {
	//   if (this._socket) {
	//     this._refreshCallback = async ({ target, data }) => {
	//       if (target === this._key) {
	//         callback(data);
	//       }
	//     };
	//     this._socket.on("refresh", this._refreshCallback);
	//   } else {
	//     throw new Error("Socket not available");
	//   }
	// }

	// offRefresh() {
	//   if (this._socket && this._refreshCallback) {
	//     this._socket.off("refresh", this._refreshCallback);
	//   }
	// }

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
