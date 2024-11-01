import { createClient, Dict } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;
let client2;
let dct;

beforeEach(async () => {
  client = createClient({ url: ZNSOCKET_URL });
  client2 = createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
  await client.flushAll();
  await client.disconnect();
  await client2.disconnect();
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

test("native_dict_setitem_socket_callback_self", async () => {
  let callback_value = false;
  dct = new Dict({ client: client, key: "dict:test" });
  dct.onRefresh((data) => {
    callback_value = data;
  });
  await dct.setitem("key", "value");
  await new Promise(resolve => setTimeout(resolve, 100));
  // we don't want the callback to trigger for the self-instance
  expect(callback_value).toBe(false);
});

test("native_dict_setitem_socket_callback", async () => {
  let callback_value = false;
  dct = new Dict({ client: client, key: "dict:test" });
  let dct2 = new Dict({ client: client2, key: "dict:test" });
  dct2.onRefresh((data) => {
    callback_value = data;
  });
  await dct.setitem("key", "value");
  await new Promise(resolve => setTimeout(resolve, 100));
  expect(callback_value).toEqual({ keys: ["key"] });
});


test("native_dict_items", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.setitem("5", "A5");
  await dct.setitem("6", "A6");

  const items = await dct.items();
  expect(items).toEqual([["5", "A5"], ["6", "A6"]]);
});

test("native_dict_keys", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.setitem("5", "A5");
  await dct.setitem("6", "A6");

  const keys = await dct.keys();
  expect(keys).toEqual(["5", "6"]);
});

test("native_dict_values", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.setitem("5", "A5");
  await dct.setitem("6", "A6");

  const values = await dct.values();
  expect(values).toEqual(["A5", "A6"]);
});

test("native_dict_clear", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.setitem("5", "A5");
  await dct.setitem("6", "A6");

  expect(await dct.items()).toEqual([["5", "A5"], ["6", "A6"]]);

  await dct.clear();
  const items = await dct.items();
  expect(items).toEqual([]);
});

test("native_dict_update", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  await dct.update({ "5": "A5", "6": "A6" });

  const items = await dct.items();
  expect(items).toEqual([["5", "A5"], ["6", "A6"]]);

  await dct.update({ "6": "B6", "7": "B7" });
  const items2 = await dct.items();
  expect(items2).toEqual([["5", "A5"], ["6", "B6"], ["7", "B7"]]);
});

test("native_dict_update_socket_callback_self", async () => {
  let callback_value = false;
  dct = new Dict({ client: client, key: "dict:test" });
  dct.onRefresh((data) => {
    callback_value = data;
  });
  await dct.update({ "5": "A5", "6": "A6" });
  await new Promise(resolve => setTimeout(resolve, 100));
  expect(callback_value).toBe(false);
});

test("native_dict_update_socket_callback", async () => {
  let callback_value = false;
  dct = new Dict({ client: client, key: "dict:test" });
  let dct2 = new Dict({ client: client2, key: "dict:test" });
  dct2.onRefresh((data) => {
    callback_value = data;
  });
  await dct.update({ "1": "A5", "2": "A6" });
  // TODO: there is an issue with int -> string conversion
  await new Promise(resolve => setTimeout(resolve, 100));
  expect(callback_value).toEqual({ keys: ["1", "2"] });

  await dct.update({ "2": "B6", "3": "B7" });
  await new Promise(resolve => setTimeout(resolve, 100));
  // TODO: there is an issue with int -> string conversion
  expect(callback_value).toEqual({ keys: ["2", "3"] });

  expect(await dct.items()).toEqual([["1", "A5"], ["2", "B6"], ["3", "B7"]]);
});

test("native_dict_getitem", async () => {
  dct = new Dict({ client: client, key: "dict:test" });

  dct.setitem("a", "A5");
  dct.setitem("b", "A6");

  expect(await dct["a"]).toBe("A5");
  expect(await dct["b"]).toBe("A6");

  await dct.update({ "5": "A5", "6": "A6" });
  expect(await dct["5"]).toBe("A5");
  expect(await dct["6"]).toBe("A6");

});


test("native_dict_setitem_x", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  dct["A"] = "5"; // right now only works with strings
  dct.B = "6";
  dct["C"] = "7";
  await new Promise(resolve => setTimeout(resolve, 100));
  expect(await dct.getitem("A")).toBe("5");
  expect(await dct.getitem("B")).toBe("6");
  expect(await dct.getitem("C")).toBe("7");
});

test("native_dict_object_keys", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  dct["5"] = "A";
  dct["6"] = "B";
  dct["7"] = "C";
  expect(await dct.keys()).toEqual(["5", "6", "7"]);
});

test("native_dict_object_values", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  dct["5"] = "A";
  dct["6"] = "B";
  dct["7"] = "C";
  expect(await dct.values()).toEqual(["A", "B", "C"]);
});

test("native_dict_object_entries", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  dct["5"] = "5";
  dct["6"] = "6";
  dct["7"] = "7";
  expect(await dct.items()).toEqual([["5", "5"], ["6", "6"], ["7", "7"]]);
});

// TODO: test int:int, int:str, str:int, str:str key:value pairs
test("native_dict_json_test", async () => {
  dct = new Dict({ client: client, key: "dict:test" });
  dct[5] = 5;
  dct[6] = "6";
  dct["7"] = 7;
  dct["8"] = "8";
  // expect(await dct.keys()).toEqual([5, 6, "7", "8"]);
  // expect(await dct.keys()).toEqual(["5", "6", "7", "8"]); // WRONG!  but what we get right now
  expect(await dct.values()).toEqual([5, "6", 7, "8"]);
  // expect(await dct.items()).toEqual([[5, 5], [6, "6"], [7, 7], [8, "8"]]);
  // expect(await dct.items()).toEqual([["5", 5], ["6", "6"], ["7", 7], ["8", "8"]]); // WRONG!  but what we get right now
});

// !! keys are for some reason always strings