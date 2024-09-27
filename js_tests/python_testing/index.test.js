import { List, Client } from "znsocket";
import { createClient } from "redis";

let client;

beforeEach(async () => {
  client = new Client({ url: process.env.ZNSOCKET_URL });
});

afterEach(async () => {
  await client.close();
});

test("test_client_lLen", async () => {
  expect(await client.lLen("list:test")).toBe(2);
});

test("test_client_lIndex", async () => {
  expect(await client.lIndex("list:test", 0)).toBe("element1");
  expect(await client.lIndex("list:test", 1)).toBe("element2");
});

test("test_client_lSet", async () => {
  expect(await client.lIndex("list:test", 0)).toBe("element1");
  expect(await client.lSet("list:test", 0, "element0")).toBe("OK");
  expect(await client.lIndex("list:test", 0)).toBe("element0");
});

test("test_client_lRem", async () => {
  expect(await client.lLen("list:test")).toBe(3);
  expect(await client.lRem("list:test", 1, "element1")).toBe("OK");
  expect(await client.lLen("list:test")).toBe(2);
});

test("test_client_rPush", async () => {
  expect(await client.rPush("list:test", "element0")).toBe("OK");
  expect(await client.rPush("list:test", "element1")).toBe("OK");
  expect(await client.lLen("list:test")).toBe(2);
  expect(await client.lIndex("list:test", 0)).toBe("element0");
  expect(await client.lIndex("list:test", 1)).toBe("element1");
});

test("test_client_lPush", async () => {
  expect(await client.lPush("list:test", "element0")).toBe("OK");
  expect(await client.lPush("list:test", "element1")).toBe("OK");
  expect(await client.lLen("list:test")).toBe(2);
  expect(await client.lIndex("list:test", 0)).toBe("element1");
  expect(await client.lIndex("list:test", 1)).toBe("element0");
});

test("test_list_append_redis", async () => {
  const r = createClient();
  r.on("error", (err) => console.error("Redis Client Error", err));
  await r.connect();

  const lst = new List(r, "list:test");
  try {
    // Test length of the list
    const length = await lst.len();
    expect(length).toBe(5); // Jest uses `expect()` for assertions
    for (let i = 0; i < length; i++) {
      expect(await lst.getitem(i)).toBe(i);
    }

    await lst.append(5);
  } finally {
    await r.disconnect();
  }
});

test("test_list_append_znsocket", async () => {
  const lst = new List(client, "list:test");
  // Test length of the list
  const length = await lst.len();
  expect(length).toBe(5); // Jest uses `expect()` for assertions
  for (let i = 0; i < length; i++) {
    expect(await lst.getitem(i)).toBe(i);
  }

  await lst.append(5);
});
