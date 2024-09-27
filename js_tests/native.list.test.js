import { createClient, List } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;
let lst;

beforeEach(async () => {
  client = new createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
  await client.flushall();
  await client.close();
});

test("native_list_append_callback", async () => {
  let callback_value = false;
  const callbacks = {
    append: async (value) => {
      callback_value = true;
    },
  };
  lst = new List({ client: client, key: "list:test", callbacks: callbacks });
  await lst.append(5);
  expect(callback_value).toBe(true);
});

test("native_list_setitem_callback", async () => {
  let callback_value = false;
  const callbacks = {
    setitem: async (value) => {
      callback_value = true;
    },
  };
  lst = new List({ client: client, key: "list:test", callbacks: callbacks });
  await lst.append(5);
  expect(callback_value).toBe(false);
  await lst.setitem(0, 5);
  expect(callback_value).toBe(true);
});
