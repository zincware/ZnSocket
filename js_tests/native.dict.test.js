import { createClient, Dict } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;
let dct;

beforeEach(async () => {
  client = createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
  await client.flushAll();
  await client.disconnect();
});

test("native_dict_setitem_callback", async () => {
  let callback_value = false;
  const callbacks = {
    setitem: async (value) => {
      callback_value = true;
    },
  };
  dct = new Dict({ client: client, key: "dict:test", callbacks: callbacks });
  await dct.setitem("key", "value");
  expect(callback_value).toBe(true);
});

test("native_dict_setitem_socket_callback", async () => {
  let callback_value = false;
  dct = new Dict({ client: client, key: "dict:test" });
  dct.onRefresh((data) => {
    callback_value = data;
  });
  await dct.setitem("key", "value");
  expect(callback_value).toEqual({ keys: ["key"] });
});

test("native_dict_items", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.setitem(5, "A5");
  await dct.setitem(6, "A6");

  const items = await dct.items();
  expect(items).toEqual([[5, "A5"], [6, "A6"]]);
});

test("native_dict_keys", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.setitem(5, "A5");
  await dct.setitem(6, "A6");

  const keys = await dct.keys();
  expect(keys).toEqual([5, 6]);
});

test("native_dict_values", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.setitem(5, "A5");
  await dct.setitem(6, "A6");

  const values = await dct.values();
  expect(values).toEqual(["A5", "A6"]);
});
