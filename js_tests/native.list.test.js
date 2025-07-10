import { createClient, Dict, List } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;
let client2;
let lst;

beforeEach(async () => {
	client = createClient({ url: ZNSOCKET_URL });
	client2 = createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
	await client.flushAll();
	await client.disconnect();
	await client2.disconnect();
});

// not supported yet
// test("native_list_push_callback", async () => {
//   let callback_value = false;
//   const callbacks = {
//     push: async (value) => {
//       callback_value = true;
//     },
//   };
//   lst = new List({ client: client, key: "list:test", callbacks: callbacks });
//   await lst.push(5);
//   expect(callback_value).toBe(true);
// });

test("native_list_set_callback", async () => {
	let callback_value = false;
	const callbacks = {
		set: async (value) => {
			callback_value = true;
		},
	};
	lst = new List({ client: client, key: "list:test", callbacks: callbacks });
	await lst.push(5);
	expect(callback_value).toBe(false);
	await lst.set(0, 5);
	expect(callback_value).toBe(true);
});

test("native_list_iter", async () => {
	lst = new List({ client: client, key: "list:test" });

	// Add initial data to the list
	await lst.push("item1");
	await lst.push("item2");
	await lst.push("item3");

	const result = [];

	// Iterate asynchronously through the list using asyncIterator
	for await (const item of lst) {
		// Capture the iteration results for assertion
		result.push(item);

		if (result.length > 100) {
			throw new Error("did not terminate"); // Failsafe to prevent test timeout due to infinite loop
		}
	}

	// Assert the collected results (ensure it matches what was initially added)
	expect(result).toEqual(["item1", "item2", "item3"]);

	await lst.push("item4");
	// Optionally, you can check that the items were re-pushed
	const newResult = [];
	for await (const item of lst) {
		newResult.push(item);
	}

	expect(newResult).toEqual(["item1", "item2", "item3", "item4"]);
});

test("native_list_get", async () => {
	lst = new List({ client: client, key: "list:test" });
	await lst.push(5);
	expect(await lst.get(0)).toBe(5);
	expect(await lst.get(1)).toBe(null);
});

test("native_list_length", async () => {
	lst = new List({ client: client, key: "list:test" });
	await lst.push(5);
	expect(await lst.length()).toBe(1);
	await lst.push(5);
	expect(await lst.length()).toBe(2);

	const lst2 = new List({ client: client, key: "list:test2" });
	expect(await lst2.length()).toBe(0);
});

test("native_list_push_socket_callback_self", async () => {
	let callback_value = false;
	lst = new List({ client: client, key: "list:test" });
	lst.onRefresh((data) => {
		callback_value = data;
	});
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toBe(false);
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toBe(false);
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toBe(false);
});

test("native_list_push_socket_callback", async () => {
	let callback_value = false;
	lst = new List({ client: client, key: "list:test" });
	const lst2 = new List({ client: client2, key: "list:test" });
	lst2.onRefresh((data) => {
		callback_value = data;
	});
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toEqual({ start: 0 });

	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toEqual({ start: 1 });

	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toEqual({ start: 2 });
});

test("native_list_set_socket_callback_self", async () => {
	let callback_value = false;
	lst = new List({ client: client, key: "list:test" });
	lst.onRefresh((data) => {
		callback_value = data;
	});
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toBe(false);
	await lst.set(0, 5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toBe(false);
});

test("native_list_set_socket_callback", async () => {
	let callback_value = false;
	lst = new List({ client: client, key: "list:test" });
	const lst2 = new List({ client: client2, key: "list:test" });
	lst2.onRefresh((data) => {
		callback_value = data;
	});
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toEqual({ start: 0 });

	await lst.set(0, 5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toEqual({ indices: [0] });
});

test("native_list_clear_socket_callback_self", async () => {
	let callback_value = false;
	lst = new List({ client: client, key: "list:test" });
	lst.onRefresh((data) => {
		callback_value = data;
	});
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toBe(false);
	await lst.clear();
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toBe(false);

	expect(await lst.length()).toBe(0);
});

test("native_list_clear_socket_callback", async () => {
	let callback_value = false;
	lst = new List({ client: client, key: "list:test" });
	const lst2 = new List({ client: client2, key: "list:test" });
	lst2.onRefresh((data) => {
		callback_value = data;
	});
	await lst.push(5);
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toEqual({ start: 0 });

	await lst.clear();
	await new Promise((resolve) => setTimeout(resolve, 100));
	expect(callback_value).toEqual({ start: 0 });
});

test("native_list_get_via_index", async () => {
	lst = new List({ client: client, key: "list:test" });

	// push an item to the list
	await lst.push("testItem");

	// Access item directly using array-like syntax
	const item = await lst[0];
	expect(item).toBe("testItem");

	// Access an out-of-bounds index, expecting null
	const outOfBoundsItem = await lst[1];
	expect(outOfBoundsItem).toBe(null);
});

test("native_list_set_via_index", async () => {
	lst = new List({ client: client, key: "list:test" });

	// push an initial item
	await lst.push("initialItem");
	expect(await lst[0]).toBe("initialItem");

	// Modify the item at index 0 directly using array-like syntax
	lst[0] = "updatedItem";
	await new Promise((resolve) => setTimeout(resolve, 100));

	// Verify the item was updated
	const item = await lst[0];
	expect(item).toBe("updatedItem");
});

test("native_list_slice", async () => {
	const jslst = ["item1", "item2", "item3"];
	let jsSliced = jslst.slice(0, 2);
	// Assert the sliced list
	expect(jsSliced).toEqual(["item1", "item2"]);
	jsSliced = jslst.slice(0, 1);
	expect(jsSliced).toEqual(["item1"]);
	jsSliced = jslst.slice(1, 2);
	expect(jsSliced).toEqual(["item2"]);
	jsSliced = jslst.slice(1, 3);
	expect(jsSliced).toEqual(["item2", "item3"]);
	// now with out list
	lst = new List({ client: client, key: "list:test" });

	// Add initial data to the list
	await lst.push("item1");
	await lst.push("item2");
	await lst.push("item3");
	// expect the full list
	expect(await lst.slice(0, 3)).toEqual(["item1", "item2", "item3"]);

	// Slice the list
	let sliced = await lst.slice(0, 2);
	// Assert the sliced list
	expect(sliced).toEqual(["item1", "item2"]);

	sliced = await lst.slice(0, 1);
	expect(sliced).toEqual(["item1"]);
	sliced = await lst.slice(1, 2);
	expect(sliced).toEqual(["item2"]);
	sliced = await lst.slice(1, 3);
	expect(sliced).toEqual(["item2", "item3"]);
});

test("native_list_in_list", async () => {
	const lst1 = new List({ client: client, key: "list:test:1" });
	const lst2 = new List({ client: client, key: "list:test:2" });

	lst1.push(1);
	lst2.push(lst1);

	await new Promise((resolve) => setTimeout(resolve, 100));

	const item = await lst2.get(0);
	expect(item).toBeInstanceOf(List);
	expect(await item.get(0)).toBe(1);
});

test("native_list_with_dict", async () => {
	const lst = new List({ client: client, key: "list:test" });
	const dct = new Dict({ client: client, key: "dict:test" });

	await dct.set("a", "A5");
	await dct.set("b", "A6");

	await lst.push(dct);

	await new Promise((resolve) => setTimeout(resolve, 100));

	const item = await lst.get(0);
	expect(item).toBeInstanceOf(Dict);
	expect(await item.get("a")).toBe("A5");
	expect(await item.get("b")).toBe("A6");
});
