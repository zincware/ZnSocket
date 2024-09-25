import { List, Client } from 'znsocket';
import { createClient } from 'redis';
import { io } from "socket.io-client";

let r; // Declare Redis client outside for global access

// Set up the Redis client before each test
beforeEach(async () => {
    r = createClient();
    r.on('error', err => console.error('Redis Client Error', err));
    await r.connect();
});

// Clean up and disconnect the Redis client after each test
afterEach(async () => {
    if (r) {
        await r.disconnect();
    }
});


// import { io } from "socket.io-client"; // Assuming you have this dependency

test("test_client_lLen", async () => {
    // Step 1: Connect to the socket at process.env.ZNSOCKET_URL
    const socket = io(process.env.ZNSOCKET_URL + "/znsocket", );

    // Step 2: Wait for the connection to be established
    await new Promise((resolve, reject) => {
        socket.on("connect", () => {
            console.log("Socket connected successfully");
            resolve();
        });
        socket.on("connect_error", (err) => {
            reject(new Error(`Failed to connect: ${err.message}`));
        });
    });

    // Step 3: Create a promise for the 'llen' event
    const lengthPromise = new Promise((resolve, reject) => {
        socket.emit("llen", {"name": "list:test"}, (data) => {
            // Handle error or invalid response
            if (data && data.error) {
                reject(new Error(data.error));
            } else {
                resolve(data); // Assume `data` contains the list length
            }
        });
    });

    // // Step 4: Await the length of the list
    const length = await lengthPromise;
    try {
        expect(length).toBe(5);
    } finally {
        socket.close();
    }
});

test('test_list_append', async () => {
    const lst = new List(r, "list:test");

    // Test length of the list
    const length = await lst.len();
    expect(length).toBe(5);  // Jest uses `expect()` for assertions
    for (let i = 0; i < length; i++) {
        expect(await lst.getitem(i)).toBe(i);
    }

    await lst.append(5);
});
