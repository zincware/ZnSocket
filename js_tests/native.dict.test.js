import { createClient, Dict } from "znsocket";

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

test("native_dict_setitem_callback", async () => {
  let callback_value = false;
  const callbacks = {
    setitem: async (value) => {
      callback_value = true;
    },
  };
  lst = new Dict({ client: client, key: "list:test", callbacks: callbacks });
  await lst.setitem("key", "value");
  expect(callback_value).toBe(true);
});
