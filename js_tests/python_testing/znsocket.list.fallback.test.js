import { test, expect, beforeEach, afterEach } from "bun:test";
import { List, Dict, createClient } from "znsocket";

let client;
let lst;

beforeEach(async () => {
  client = createClient({ url: process.env.ZNSOCKET_URL });
  client.on("error", (err) => console.error("Redis Client Error", err));
  await client.connect();
  lst = new List({ client: client, key: "list:test", fallback: "list:test:fallback" });
});

afterEach(async () => {
  await client.disconnect();
});

test("test_empty_list_adapter_fallback", async () => {
  // Test length of the list
  const length = await lst.length();
  expect(length).toBe(5); // Jest uses `expect()` for assertions
});

