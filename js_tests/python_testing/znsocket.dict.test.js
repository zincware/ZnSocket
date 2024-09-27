import { createClient, Dict } from "znsocket";

let client;

beforeEach(async () => {
  client = new createClient({ url: process.env.ZNSOCKET_URL });
  client.on("error", (err) => console.error("Redis Client Error", err));
  await client.connect();
});

afterEach(async () => {
  await client.close();
});

test("test_dict_keys_single_znsocket", async () => {
  const dct = new Dict(client, "dict:test");
  expect(await dct.keys()).toEqual(["a"]);
});


test("test_dict_keys_multiple_znsocket", async () => {
  const dct = new Dict(client, "dict:test");
  expect(await dct.keys()).toEqual(["a", "c"]);
});


test("test_dict_values_single_znsocket", async () => {
  const dct = new Dict(client, "dict:test");
  expect(await dct.values()).toEqual(["string"]);
});


test("test_dict_values_multiple_znsocket", async () => {
  const dct = new Dict(client, "dict:test");
  expect(await dct.values()).toEqual([{"lorem": "ipsum"}, 25]);
});


test("test_dict_items_znsocket", async () => {
  const dct = new Dict(client, "dict:test");
  expect(await dct.items()).toEqual([("a", {"lorem": "ipsum"}), ("b", 25)]);
});

test("test_dict_getitem_znsocket", async () => {
  const dct = new Dict(client, "dict:test");
  expect(await dct.getitem("b")).toBe(25);
  expect(await dct.getitem("a")).toEqual({"lorem": "ipsum"});
});


test("test_dict_setitem_znsocket", async () => {
  const dct = new Dict(client, "dict:test");
  await dct.setitem("b", 25)
  await dct.setitem("a", {"lorem": "ipsum"})
  expect(await dct.getitem("b")).toBe(25);
  expect(await dct.getitem("a")).toEqual({"lorem": "ipsum"});
});
