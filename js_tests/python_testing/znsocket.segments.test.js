import { afterEach, beforeEach, expect, test } from "bun:test";
import { createClient, Segments } from "znsocket";

let client;
let lst;

beforeEach(async () => {
	client = createClient({ url: process.env.ZNSOCKET_URL });
	client.on("error", (err) => console.error("Redis Client Error", err));
	await client.connect();
	lst = new Segments({ client: client, key: "segments:test" });
});

afterEach(async () => {
	await client.disconnect();
});

test("test_segments_len_znsocket", async () => {
	// Test length of the list
	const length = await lst.length();
	expect(length).toBe(5); // Jest uses `expect()` for assertions
});

test("test_segments_getitem_znsocket", async () => {
	// Test getting each item in the length of the list
	const length = await lst.length();
	for (let i = 0; i < length; i++) {
		const item = await lst.get(i);
		expect(item).toEqual(i);
	}
});
