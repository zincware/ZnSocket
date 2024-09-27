import { List, createClient } from "znsocket";

let client;

beforeEach(async () => {
  client = new createClient({ url: process.env.ZNSOCKET_URL });
});

afterEach(async () => {
  await client.close();
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
