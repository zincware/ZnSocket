import { List, createClient } from "znsocket";

let client;
let lst;

beforeEach(async () => {
  client = createClient({ url: process.env.ZNSOCKET_URL });
  client.on("error", (err) => console.error("Redis Client Error", err));
  await client.connect();
  lst = new List({ client: client, key: "list:test" });
});

afterEach(async () => {
  await client.disconnect();
});

test("test_list_push_znsocket", async () => {
  // Test length of the list
  const length = await lst.length();
  expect(length).toBe(5); // Jest uses `expect()` for assertions
  for (let i = 0; i < length; i++) {
    expect(await lst.get(i)).toBe(i);
  }
  await lst.push(5);
});
