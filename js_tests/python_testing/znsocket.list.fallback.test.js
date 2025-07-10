import { afterEach, beforeEach, expect, test } from "bun:test";
import { createClient, Dict, List } from "znsocket";

let client;
let lst;

beforeEach(async () => {
	client = createClient({ url: process.env.ZNSOCKET_URL });
	client.on("error", (err) => console.error("Redis Client Error", err));
	await client.connect();
	lst = new List({
		client: client,
		key: "list:test",
		fallback: "list:test:fallback",
	});
});

afterEach(async () => {
	await client.disconnect();
});

test("test_empty_list_adapter_fallback", async () => {
	// Test length of the list
	const length = await lst.length();
	expect(length).toBe(5); // Jest uses `expect()` for assertions
});

test("test_list_fallback_frozen_get", async () => {
	const val0 = await lst.get(0);
	expect(val0).toBe(10);
	const val4 = await lst.get(4);
	expect(val4).toBe(50);
});

test("test_list_fallback_frozen_slice", async () => {
	const slice = await lst.slice(1, 4);
	expect(slice).toEqual([20, 30, 40]);
});

test("test_list_fallback_copy_len", async () => {
	const length = await lst.length();
	expect(length).toBe(5);
});

test("test_list_fallback_copy_get", async () => {
	const val0 = await lst.get(0);
	expect(val0).toBe(10);
	const val4 = await lst.get(4);
	expect(val4).toBe(50);
});

test("test_list_fallback_copy_slice", async () => {
	const slice = await lst.slice(1, 4);
	expect(slice).toEqual([20, 30, 40]);
});
