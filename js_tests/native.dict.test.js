import { createClient, Dict } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;
let lst;

beforeEach(async () => {
  client = createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
  await client.flushAll();
  await client.disconnect();
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


test("native_dict_setitem_socket_callback", async () => {
  let callback_value = false;
  lst = new Dict({ client: client, key: "list:test", socket: client._socket });
  lst.add_refresh_listener((data) => {
    callback_value = data;
  });
  await lst.setitem("key", "value");
  expect(callback_value).toEqual({"keys": ["key"]});
});
