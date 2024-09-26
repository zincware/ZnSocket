import { List, Client } from "znsocket";
import { createClient } from "redis";

const ZNSOCKET_URL = "http://127.0.0.1:4748";

test("native_client_rPush_lLen", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    await client.rPush("list:test", "a");
    expect(await client.lLen("list:test")).toBe(1);
  } finally {
    await client.flushall();
    await client.close();
  }
});

test("native_client_lIndex_lSet", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    // Push two elements to the list
    await client.rPush("list:test", "a");
    await client.rPush("list:test", "b");

    // Check that the first element is "a"
    expect(await client.lIndex("list:test", 0)).toBe("a");

    // Set the first element to "c"
    await client.lSet("list:test", 0, "c");

    // Verify that the first element is now "c"
    expect(await client.lIndex("list:test", 0)).toBe("c");
  } finally {
    await client.flushall();
    await client.close();
  }
});

test("native_client_lRem", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    // Push three elements to the list
    await client.rPush("list:test", "a");
    await client.rPush("list:test", "b");
    await client.rPush("list:test", "a");

    // Remove one occurrence of "a"
    await client.lRem("list:test", 1, "a");

    // Check that the length of the list is now 2
    expect(await client.lLen("list:test")).toBe(2);

    // Verify that the first element is now "b"
    expect(await client.lIndex("list:test", 0)).toBe("b");
  } finally {
    await client.flushall();
    await client.close();
  }
});

test("native_client_lPush", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    // Push two elements to the left of the list
    await client.lPush("list:test", "a");
    await client.lPush("list:test", "b");

    // Verify that the first element is now "b" (lPush pushes to the front)
    expect(await client.lIndex("list:test", 0)).toBe("b");
    expect(await client.lIndex("list:test", 1)).toBe("a");
  } finally {
    await client.flushall();
    await client.close();
  }
});

test("native_client_hSet_hGet", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    // Set a hash field value
    await client.hSet("hash:test", "field1", "value1");

    // Get the hash field value
    expect(await client.hGet("hash:test", "field1")).toBe("value1");
  } finally {
    await client.flushall();
    await client.close();
  }
});

test("native_client_hDel_hExists", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    // Set a hash field
    await client.hSet("hash:test", "field1", "value1");

    // Verify that the field exists
    expect(await client.hExists("hash:test", "field1")).toBe(1);

    // Delete the field
    await client.hDel("hash:test", "field1");

    // Verify that the field no longer exists
    expect(await client.hExists("hash:test", "field1")).toBe(0);
  } finally {
    await client.flushall();
    await client.close();
  }
});

test("native_client_hLen_hKeys_hVals", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    // Set multiple fields in a hash
    await client.hSet("hash:test", "field1", "value1");
    await client.hSet("hash:test", "field2", "value2");

    // Verify the length of the hash
    expect(await client.hLen("hash:test")).toBe(2);

    // Verify the keys in the hash
    expect(await client.hKeys("hash:test")).toEqual(["field1", "field2"]);

    // Verify the values in the hash
    expect(await client.hVals("hash:test")).toEqual(["value1", "value2"]);
  } finally {
    await client.flushall();
    await client.close();
  }
});

test("native_client_hGetAll", async () => {
  const client = new Client(ZNSOCKET_URL);

  try {
    // Set multiple fields in a hash
    await client.hSet("hash:test", "field1", "value1");
    await client.hSet("hash:test", "field2", "value2");

    // Verify that hGetAll returns the correct map of fields to values
    expect(await client.hGetAll("hash:test")).toEqual({
      field1: "value1",
      field2: "value2",
    });
  } finally {
    await client.flushall();
    await client.close();
  }
});
