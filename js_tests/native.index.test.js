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
