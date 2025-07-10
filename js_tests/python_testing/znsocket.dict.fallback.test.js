import { afterEach, beforeEach, expect, test } from "bun:test";
import { createClient, Dict } from "znsocket";

let client;
let dct;

beforeEach(async () => {
	client = createClient({ url: process.env.ZNSOCKET_URL });
	client.on("error", (err) => console.error("Redis Client Error", err));
	await client.connect();
	dct = new Dict({
		client: client,
		key: "dict:test",
		fallback: "dict:test:fallback",
		fallbackPolicy: "frozen",
	});
});

afterEach(async () => {
	await client.disconnect();
});

test("test_dict_fallback_frozen_get", async () => {
	const valA = await dct.get("a");
	expect(valA).toBe(1);
	const valC = await dct.get("c");
	expect(valC).toBe(3);
});

test("test_dict_fallback_frozen_len", async () => {
	const length = await dct.length();
	expect(length).toBe(3);
});

test("test_dict_fallback_frozen_keys", async () => {
	const keys = await dct.keys();
	expect(keys.sort()).toEqual(["a", "b", "c"]);
});

test("test_dict_fallback_frozen_values", async () => {
	const values = await dct.values();
	expect(values.sort()).toEqual([1, 2, 3]);
});

test("test_dict_fallback_frozen_items", async () => {
	const items = await dct.entries();
	expect(items.sort()).toEqual([
		["a", 1],
		["b", 2],
		["c", 3],
	]);
});
