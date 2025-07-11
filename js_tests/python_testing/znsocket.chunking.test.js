import { afterEach, beforeEach, expect, test } from "bun:test";
import { createClient, Dict, List } from "znsocket";

let client;
let dct;
let lst;

beforeEach(async () => {
	client = createClient({ url: process.env.ZNSOCKET_URL });
	client.on("error", (err) => console.error("Redis Client Error", err));
	await client.connect();
	dct = new Dict({ client: client, key: "chunked_test_dict" });
	lst = new List({ client: client, key: "chunked_test_list" });
});

afterEach(async () => {
	await client.disconnect();
});

test("test_chunked_large_dict_python_to_js", async () => {
	// Verify that JavaScript can read the large chunked data stored by Python
	console.log("Testing JavaScript access to chunked dict data...");

	// Check that the metadata exists and is readable
	const metadata = await dct.get("metadata");
	console.log("Metadata:", metadata);

	expect(metadata).toBeDefined();
	expect(metadata.type).toBe("chunked_numpy_array");
	expect(metadata.description).toBe("Large array sent from Python with chunking");
	expect(metadata.size).toEqual([1000, 1000]);

	// Check that the large data exists and can be accessed
	const hasLargeData = await client.hExists("znsocket.Dict:chunked_test_dict", "large_data");
	expect(hasLargeData).toBe(true);

	// Try to read the large data - this should work even though it was chunked
	const largeData = await dct.get("large_data");
	console.log("Large data received, type:", typeof largeData);
	console.log("Large data keys:", Object.keys(largeData));

	// The data should be a valid object with the expected structure
	expect(largeData).toBeDefined();
	expect(typeof largeData).toBe("object");

	// It should have the numpy array structure from znjson
	expect(largeData._type).toBe("np.ndarray_b64");
	expect(largeData.value).toBeDefined();

	console.log("✅ JavaScript successfully read chunked dict data from Python!");
});

test("test_chunked_large_list_python_to_js", async () => {
	// Verify that JavaScript can read the large chunked list data stored by Python
	console.log("Testing JavaScript access to chunked list data...");

	// Check the list length
	const listLength = await lst.length();
	console.log("List length:", listLength);
	expect(listLength).toBe(800);

	// Check first and last items
	const firstItem = await lst.get(0);
	const lastItem = await lst.get(799);

	console.log("First item length:", firstItem.length);
	console.log("Last item length:", lastItem.length);

	// Each item should be a large string
	expect(firstItem).toMatch(/^item_0_x+$/);
	expect(lastItem).toMatch(/^item_799_x+$/);
	expect(firstItem.length).toBeGreaterThan(10000);
	expect(lastItem.length).toBeGreaterThan(10000);

	// Check metadata
	const metadataDict = new Dict({ client: client, key: "chunked_list_metadata" });
	const totalItems = await metadataDict.get("total_items");
	const description = await metadataDict.get("description");

	expect(totalItems).toBe(800);
	expect(description).toBe("Large list sent from Python with chunking");

	console.log("✅ JavaScript successfully read chunked list data from Python!");
});


test("test_chunked_large_list_python_to_js_2", async () => {
	// Verify that JavaScript can read the large chunked list data stored by Python
	console.log("Testing JavaScript access to chunked list data...");

	// Check the list length
	const listLength = await lst.length();
	console.log("List length:", listLength);
	expect(listLength).toBe(800);

	// Check first and last items
	const firstItem = await lst.get(0);
	const lastItem = await lst.get(799);

	console.log("First item length:", firstItem.length);
	console.log("Last item length:", lastItem.length);

	// Each item should be a large string
	expect(firstItem).toMatch(/^item_0_x/);
	expect(lastItem).toMatch(/^item_799_x/);
	expect(firstItem.length).toBeGreaterThan(10000);
	expect(lastItem.length).toBeGreaterThan(10000);

	// Check metadata
	const metadataDict = new Dict({ client: client, key: "chunked_list_metadata" });
	const totalItems = await metadataDict.get("total_items");
	const description = await metadataDict.get("description");

	expect(totalItems).toBe(800);
	expect(description).toBe("Large list sent from Python with chunking");

	console.log("✅ JavaScript successfully read chunked list data from Python!");
});
