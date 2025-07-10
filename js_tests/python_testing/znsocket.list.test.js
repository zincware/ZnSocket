import { afterEach, beforeEach, expect, test } from "bun:test";
import { createClient, Dict, List } from "znsocket";

let client;
let lst;

beforeEach(async () => {
	client = createClient({ url: process.env.ZNSOCKET_URL });
	client.on("error", (err) => console.error("Redis Client Error", err));
	await client.connect();
	lst = new List({ client: client, key: "list:test" });
});

afterEach(async () => {
	await client.disconnect();
});

test("test_list_push_znsocket", async () => {
	// Test length of the list
	const length = await lst.length();
	expect(length).toBe(5); // Jest uses `expect()` for assertions
	for (let i = 0; i < length; i++) {
		expect(await lst.get(i)).toBe(i);
	}
	await lst.push(5);
});

test("test_list_with_list_and_dict", async () => {
	const referencedList = await lst.get(0);
	const referencedDict = await lst.get(1);

	expect(referencedList._key).toBe("znsocket.List:list:referenced");
	expect(referencedDict._key).toBe("znsocket.Dict:dict:referenced");

	expect(referencedList).toBeInstanceOf(List);
	expect(referencedDict).toBeInstanceOf(Dict);

	expect(await referencedList.get(0)).toBe("Hello World");
	expect(await referencedDict.get("key")).toBe("value");

	await referencedList.push("New Value");
	await referencedDict.set("new_key", "new_value");
});

test("test_list_adapter_znsocket", async () => {
	// Wait a moment for adapter detection to complete
	await new Promise((resolve) => setTimeout(resolve, 100));

	const length = await lst.length();
	expect(length).toBe(4);

	// Test individual items
	expect(await lst.get(0)).toBe(1);
	expect(await lst.get(1)).toBe(2);
	expect(await lst.get(2)).toBe(3);
	expect(await lst.get(3)).toBe(4);
});

test("test_list_adapter_slice_basic", async () => {
	// Wait a moment for adapter detection to complete
	await new Promise((resolve) => setTimeout(resolve, 100));

	// Test basic slicing (JavaScript slice is end-exclusive)
	expect(await lst.slice(1, 4)).toEqual([2, 3, 4]);
	expect(await lst.slice(0, 3)).toEqual([1, 2, 3]);
	expect(await lst.slice(7, 10)).toEqual([8, 9, 10]);
});

test("test_list_adapter_slice_with_step", async () => {
	// Wait a moment for adapter detection to complete
	await new Promise((resolve) => setTimeout(resolve, 100));

	// Test slicing with step
	expect(await lst.slice(0, 10, 2)).toEqual([1, 3, 5, 7, 9]);
	expect(await lst.slice(1, 8, 2)).toEqual([2, 4, 6, 8]);
});

test("test_list_adapter_slice_negative_indices", async () => {
	// Wait a moment for adapter detection to complete
	await new Promise((resolve) => setTimeout(resolve, 100));

	const length = await lst.length();

	// Test slicing with negative indices
	expect(await lst.slice(-3, length)).toEqual([8, 9, 10]);
	expect(await lst.slice(0, -2)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
	expect(await lst.slice(-5, -1)).toEqual([6, 7, 8, 9]);
});
