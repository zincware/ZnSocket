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

test("test_list_adapter_with_segments", async () => {
	// Verify the ListAdapter data setup from the Python test
	const lst = new List({ client: client, key: "test:adapter_data" });

	// Verify initial adapter data length and content
	const listLength = await lst.length();
	expect(listLength).toBe(5);

	// Check original adapted data values
	const item0 = await lst.get(0);
	const item1 = await lst.get(1);
	const item2 = await lst.get(2);
	const item3 = await lst.get(3);
	const item4 = await lst.get(4);

	expect(item0).toEqual({ name: "item_0", score: 85, category: "A" });
	expect(item1).toEqual({ name: "item_1", score: 92, category: "B" });
	expect(item2).toEqual({ name: "item_2", score: 78, category: "A" });
	expect(item3).toEqual({ name: "item_3", score: 96, category: "C" });
	expect(item4).toEqual({ name: "item_4", score: 83, category: "B" });

	// Verify the Python segment modifications are accessible
	// Check if the modified segment Dict exists (item 2)
	const modifiedSegmentDict2 = new Dict({ client: client, key: "test:adapter_segments/2" });
	expect(await modifiedSegmentDict2.get("name")).toBe("item_2_modified");
	expect(await modifiedSegmentDict2.get("score")).toBe(95);
	expect(await modifiedSegmentDict2.get("category")).toBe("A+");
	expect(await modifiedSegmentDict2.get("modified")).toBe(true);
	expect(await modifiedSegmentDict2.get("source")).toBe("segments_copy");

	// Check if the modified segment Dict exists (item 4)
	const modifiedSegmentDict4 = new Dict({ client: client, key: "test:adapter_segments/4" });
	expect(await modifiedSegmentDict4.get("name")).toBe("item_4_enhanced");
	expect(await modifiedSegmentDict4.get("score")).toBe(99);
	expect(await modifiedSegmentDict4.get("category")).toBe("S");
	expect(await modifiedSegmentDict4.get("enhanced")).toBe(true);
	expect(await modifiedSegmentDict4.get("multiplier")).toBe(1.2);

	// Verify that original adapter data is still unchanged
	const stillOriginalItem2 = await lst.get(2);
	const stillOriginalItem4 = await lst.get(4);
	expect(stillOriginalItem2).toEqual({ name: "item_2", score: 78, category: "A" });
	expect(stillOriginalItem4).toEqual({ name: "item_4", score: 83, category: "B" });

	// Test JavaScript-side modification - create another segment copy
	const jsModifiedDict = new Dict({ client: client, key: "test:adapter_segments/1_js" });
	await jsModifiedDict.clear();
	await jsModifiedDict.set("name", "item_1_js_enhanced");
	await jsModifiedDict.set("score", 100);
	await jsModifiedDict.set("category", "S+");
	await jsModifiedDict.set("enhanced_by", "javascript");
	await jsModifiedDict.set("bonus_points", 15);

	// Verify the JavaScript modification
	expect(await jsModifiedDict.get("name")).toBe("item_1_js_enhanced");
	expect(await jsModifiedDict.get("score")).toBe(100);
	expect(await jsModifiedDict.get("category")).toBe("S+");
	expect(await jsModifiedDict.get("enhanced_by")).toBe("javascript");
	expect(await jsModifiedDict.get("bonus_points")).toBe(15);

	// Verify original list item 1 is still unchanged
	const originalItem1 = await lst.get(1);
	expect(originalItem1).toEqual({ name: "item_1", score: 92, category: "B" });

	// Test another JavaScript copy-on-write modification (item 0)
	const anotherJsDict = new Dict({ client: client, key: "test:adapter_segments/0_js_copy" });
	await anotherJsDict.clear();
	await anotherJsDict.set("name", "item_0_upgraded");
	await anotherJsDict.set("score", 90);
	await anotherJsDict.set("category", "A++");
	await anotherJsDict.set("upgrade_reason", "javascript_processing");
	await anotherJsDict.set("timestamp", new Date().toISOString());

	// Verify this modification too
	expect(await anotherJsDict.get("name")).toBe("item_0_upgraded");
	expect(await anotherJsDict.get("score")).toBe(90);
	expect(await anotherJsDict.get("category")).toBe("A++");
	expect(await anotherJsDict.get("upgrade_reason")).toBe("javascript_processing");

	// Verify original item 0 is still unchanged
	const originalItem0 = await lst.get(0);
	expect(originalItem0).toEqual({ name: "item_0", score: 85, category: "A" });

	// Verify all original adapter data remains unchanged
	const allOriginalData = [];
	for (let i = 0; i < await lst.length(); i++) {
		allOriginalData.push(await lst.get(i));
	}

	expect(allOriginalData).toEqual([
		{ name: "item_0", score: 85, category: "A" },
		{ name: "item_1", score: 92, category: "B" },
		{ name: "item_2", score: 78, category: "A" },  // Original unchanged
		{ name: "item_3", score: 96, category: "C" },
		{ name: "item_4", score: 83, category: "B" }   // Original unchanged
	]);

	// Test cross-language copy-on-write behavior verification
	// We have Python modifications at keys test:adapter_segments/2 and test:adapter_segments/4
	// We have JavaScript modifications at keys test:adapter_segments/1_js and test:adapter_segments/0_js_copy
	// All should coexist while leaving the original adapter data unchanged

	// Final verification: demonstrate that we can access both Python and JS modifications
	const pythonMod2 = new Dict({ client: client, key: "test:adapter_segments/2" });
	const pythonMod4 = new Dict({ client: client, key: "test:adapter_segments/4" });
	const jsMod1 = new Dict({ client: client, key: "test:adapter_segments/1_js" });
	const jsMod0 = new Dict({ client: client, key: "test:adapter_segments/0_js_copy" });

	// All modifications should be accessible and distinct
	expect(await pythonMod2.get("source")).toBe("segments_copy");
	expect(await pythonMod4.get("enhanced")).toBe(true);
	expect(await jsMod1.get("enhanced_by")).toBe("javascript");
	expect(await jsMod0.get("upgrade_reason")).toBe("javascript_processing");
});
