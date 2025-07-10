import { createClient, List } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;

beforeEach(async () => {
	client = createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
	await client.flushAll();
	await client.disconnect();
});

test("fallback_list_length_issue", async () => {
	// Create a source list with data
	await client.rPush("room:default:frames", JSON.stringify("frame1"));
	await client.rPush("room:default:frames", JSON.stringify("frame2"));
	await client.rPush("room:default:frames", JSON.stringify("frame3"));

	// Direct client call should return 3
	const directLength = await client.lLen("room:default:frames");
	expect(directLength).toBe(3);

	// List with fallback should also return 3
	const listWithFallback = new List({
		client,
		key: "room:default:frames",
		fallback: "room:default:frames",
	});

	const fallbackLength = await listWithFallback.length();
	expect(fallbackLength).toBe(3);
});

test("fallback_list_with_explicit_policy", async () => {
	// Create a source list with data
	await client.rPush("room:default:frames", JSON.stringify("frame1"));
	await client.rPush("room:default:frames", JSON.stringify("frame2"));
	await client.rPush("room:default:frames", JSON.stringify("frame3"));

	// List with explicit fallback policy "frozen" should work
	const listWithFrozenPolicy = new List({
		client,
		key: "room:default:frames",
		fallback: "room:default:frames",
		fallbackPolicy: "frozen",
	});

	const frozenLength = await listWithFrozenPolicy.length();
	expect(frozenLength).toBe(3);
});
