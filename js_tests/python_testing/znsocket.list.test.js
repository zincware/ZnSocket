import { List, createClient } from "znsocket";

let client;
let lst;

beforeEach(async () => {
  client = new createClient({ url: process.env.ZNSOCKET_URL });
  client.on("error", (err) => console.error("Redis Client Error", err));
  await client.connect();
  lst = new List({ client: client, key: "list:test" });
});

afterEach(async () => {
  await client.close();
});

test("test_list_append_znsocket", async () => {
  // Test length of the list
  const length = await lst.len();
  expect(length).toBe(5); // Jest uses `expect()` for assertions
  for (let i = 0; i < length; i++) {
    expect(await lst.getitem(i)).toBe(i);
  }
  await lst.append(5);
});
