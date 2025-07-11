import { afterEach, beforeEach, expect, test } from "bun:test";
import { createClient, Dict, List } from "znsocket";

let client;

beforeEach(async () => {
	client = createClient({ url: process.env.ZNSOCKET_URL });
	client.on("error", (err) => console.error("Client Error", err));
	await client.connect();
});

afterEach(async () => {
	await client.disconnect();
});

test("test_segments_with_dict", async () => {
	// Verify the data setup from the Python test
	const lst = new List({ client: client, key: "test:data" });
	
	// Verify initial list length and data
	const listLength = await lst.length();
	expect(listLength).toBe(4);
	
	// Check original data values
	const item0 = await lst.get(0);
	const item1 = await lst.get(1);
	const item2 = await lst.get(2);
	const item3 = await lst.get(3);
	
	expect(await item0.get("value")).toEqual([1, 2, 3]);
	expect(await item1.get("value")).toEqual([4, 5, 6]);
	expect(await item2.get("value")).toEqual([7, 8, 9]);
	expect(await item3.get("value")).toEqual([10, 11, 12]);
	
	// Verify the Python modifications are accessible
	// Check if the modified segment Dict exists
	const modifiedSegmentDict = new Dict({ client: client, key: "test:data/segments/2" });
	const modifiedValue = await modifiedSegmentDict.get("value");
	expect(modifiedValue).toEqual([100, 200, 300]); // Modified value from Python
	
	// Verify that original list item 2 is still unchanged
	const originalItem2 = await lst.get(2);
	expect(await originalItem2.get("value")).toEqual([7, 8, 9]); // Original unchanged
	
	// Test JavaScript-side modification - create another copy
	const jsModifiedDict = new Dict({ client: client, key: "test:data/segments/1_js" });
	await jsModifiedDict.clear();
	await jsModifiedDict.set("value", [400, 500, 600]);
	await jsModifiedDict.set("modified_by", "javascript");
	
	// Verify the JavaScript modification
	expect(await jsModifiedDict.get("value")).toEqual([400, 500, 600]);
	expect(await jsModifiedDict.get("modified_by")).toBe("javascript");
	
	// Verify original list item 1 is still unchanged
	const originalItem1 = await lst.get(1);
	expect(await originalItem1.get("value")).toEqual([4, 5, 6]); // Original unchanged
	
	// Test copy-on-write concept: create a copy of item 3 and modify it
	const originalItem3 = await lst.get(3);
	const originalValue3 = await originalItem3.get("value");
	expect(originalValue3).toEqual([10, 11, 12]);
	
	// Create a modified copy
	const copiedDict = new Dict({ client: client, key: "test:data/copy_js/3" });
	await copiedDict.clear();
	await copiedDict.set("value", [999, 888, 777]);
	await copiedDict.set("source", "javascript_copy");
	
	// Verify copy has different values
	expect(await copiedDict.get("value")).toEqual([999, 888, 777]);
	expect(await copiedDict.get("source")).toBe("javascript_copy");
	
	// Verify original is still unchanged
	const stillOriginalItem3 = await lst.get(3);
	expect(await stillOriginalItem3.get("value")).toEqual([10, 11, 12]); // Original unchanged
	
	// Verify we can access all original data
	const allOriginal = [];
	for (let i = 0; i < await lst.length(); i++) {
		const item = await lst.get(i);
		const value = await item.get("value");
		allOriginal.push({ value });
	}
	
	// Original list should be unchanged
	expect(allOriginal).toEqual([
		{ value: [1, 2, 3] },
		{ value: [4, 5, 6] },
		{ value: [7, 8, 9] },  // Original unchanged
		{ value: [10, 11, 12] }
	]);
});