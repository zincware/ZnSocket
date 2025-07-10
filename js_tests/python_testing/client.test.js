import { afterEach, beforeEach, expect, test } from "bun:test";
import { createClient } from "znsocket";

let client;

beforeEach(async () => {
	client = createClient({ url: process.env.ZNSOCKET_URL });
});

afterEach(async () => {
	await client.disconnect();
});

test("test_client_lLen", async () => {
	expect(await client.lLen("list:test")).toBe(2);
});

test("test_client_lIndex", async () => {
	expect(await client.lIndex("list:test", 0)).toBe("element1");
	expect(await client.lIndex("list:test", 1)).toBe("element2");
});

test("test_client_lSet", async () => {
	expect(await client.lIndex("list:test", 0)).toBe("element1");
	expect(await client.lSet("list:test", 0, "element0")).toBe("OK");
	expect(await client.lIndex("list:test", 0)).toBe("element0");
});

test("test_client_lRem", async () => {
	expect(await client.lLen("list:test")).toBe(3);
	expect(await client.lRem("list:test", 1, "element1")).toBe("OK");
	expect(await client.lLen("list:test")).toBe(2);
});

test("test_client_rPush", async () => {
	expect(await client.rPush("list:test", "element0")).toBe("OK");
	expect(await client.rPush("list:test", "element1")).toBe("OK");
	expect(await client.lLen("list:test")).toBe(2);
	expect(await client.lIndex("list:test", 0)).toBe("element0");
	expect(await client.lIndex("list:test", 1)).toBe("element1");
});

test("test_client_lPush", async () => {
	expect(await client.lPush("list:test", "element0")).toBe("OK");
	expect(await client.lPush("list:test", "element1")).toBe("OK");
	expect(await client.lLen("list:test")).toBe(2);
	expect(await client.lIndex("list:test", 0)).toBe("element1");
	expect(await client.lIndex("list:test", 1)).toBe("element0");
});
