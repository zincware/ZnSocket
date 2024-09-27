import { createClient } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;

beforeEach(async () => {
  client = new createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
  await client.flushall();
  await client.close();
});

test("native_client_rPush_lLen", async () => {
  await client.rPush("list:test", "a");
  expect(await client.lLen("list:test")).toBe(1);
});

test("native_client_lIndex_lSet", async () => {
  await client.rPush("list:test", "a");
  await client.rPush("list:test", "b");

  expect(await client.lIndex("list:test", 0)).toBe("a");

  await client.lSet("list:test", 0, "c");
  expect(await client.lIndex("list:test", 0)).toBe("c");
});

test("native_client_lRem", async () => {
  await client.rPush("list:test", "a");
  await client.rPush("list:test", "b");
  await client.rPush("list:test", "a");

  await client.lRem("list:test", 1, "a");

  expect(await client.lLen("list:test")).toBe(2);
  expect(await client.lIndex("list:test", 0)).toBe("b");
});

test("native_client_lPush", async () => {
  await client.lPush("list:test", "a");
  await client.lPush("list:test", "b");

  expect(await client.lIndex("list:test", 0)).toBe("b");
  expect(await client.lIndex("list:test", 1)).toBe("a");
});

test("native_client_hSet_hGet", async () => {
  await client.hSet("hash:test", "field1", "value1");
  expect(await client.hGet("hash:test", "field1")).toBe("value1");
});

test("native_client_hDel_hExists", async () => {
  await client.hSet("hash:test", "field1", "value1");

  expect(await client.hExists("hash:test", "field1")).toBe(1);

  await client.hDel("hash:test", "field1");
  expect(await client.hExists("hash:test", "field1")).toBe(0);
});

test("native_client_hLen_hKeys_hVals", async () => {
  await client.hSet("hash:test", "field1", "value1");
  await client.hSet("hash:test", "field2", "value2");

  expect(await client.hLen("hash:test")).toBe(2);
  expect(await client.hKeys("hash:test")).toEqual(["field1", "field2"]);
  expect(await client.hVals("hash:test")).toEqual(["value1", "value2"]);
});

test("native_client_hGetAll", async () => {
  await client.hSet("hash:test", "field1", "value1");
  await client.hSet("hash:test", "field2", "value2");

  expect(await client.hGetAll("hash:test")).toEqual({
    field1: "value1",
    field2: "value2",
  });
});
