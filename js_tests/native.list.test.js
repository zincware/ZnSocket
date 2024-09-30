import { createClient, List } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;
let lst;

beforeEach(async () => {
  client = createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
  await client.flushAll();
  await client.disconnect();
});

test("native_list_append_callback", async () => {
  let callback_value = false;
  const callbacks = {
    append: async (value) => {
      callback_value = true;
    },
  };
  lst = new List({ client: client, key: "list:test", callbacks: callbacks });
  await lst.append(5);
  expect(callback_value).toBe(true);
});

test("native_list_setitem_callback", async () => {
  let callback_value = false;
  const callbacks = {
    setitem: async (value) => {
      callback_value = true;
    },
  };
  lst = new List({ client: client, key: "list:test", callbacks: callbacks });
  await lst.append(5);
  expect(callback_value).toBe(false);
  await lst.setitem(0, 5);
  expect(callback_value).toBe(true);
});


test("native_list_iter", async () => {
  lst = new List({ client: client, key: "list:test" });

  // Add initial data to the list
  await lst.append("item1");
  await lst.append("item2");
  await lst.append("item3");

  const result = [];

  // Iterate asynchronously through the list using asyncIterator
  for await (let item of lst) {
    // Capture the iteration results for assertion
    result.push(item);

    if (result.length > 100) {
      throw new Error("did not terminate"); // Failsafe to prevent test timeout due to infinite loop
    }
  }

  // Assert the collected results (ensure it matches what was initially added)
  expect(result).toEqual(["item1", "item2", "item3"]);

  await lst.append("item4");
  // Optionally, you can check that the items were re-appended
  const newResult = [];
  for await (let item of lst) {
    newResult.push(item);
  }

  expect(newResult).toEqual(["item1", "item2", "item3", "item4"]);
});


test("native_list_getitem", async () => {
  lst = new List({ client: client, key: "list:test" });
  await lst.append(5);
  expect(await lst.getitem(0)).toBe(5);
  expect(await lst.getitem(1)).toBe(null);
});


test("native_list_len", async () => {
  lst = new List({ client: client, key: "list:test" });
  await lst.append(5);
  expect(await lst.len()).toBe(1);
  await lst.append(5);
  expect(await lst.len()).toBe(2);
});

test("native_list_append_socket_callback", async () => {
  let callback_value = false;
  lst = new List({ client: client, key: "list:test", socket: client._socket });
  lst.add_refresh_listener((data) => {
    callback_value = data;
  });
  await lst.append(5);
  expect(callback_value).toEqual({ start: 0 });

  await lst.append(5);
  expect(callback_value).toEqual({ start: 1 });

  await lst.append(5);
  expect(callback_value).toEqual({ start: 2 });
});

// TODO: others than append!