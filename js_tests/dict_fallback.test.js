import { createClient, Dict } from "znsocket";

const ZNSOCKET_URL = "http://127.0.0.1:4748";
let client;

beforeEach(async () => {
	client = createClient({ url: ZNSOCKET_URL });
});

afterEach(async () => {
	await client.flushAll();
	await client.disconnect();
});

test("fallback_dict_length_issue", async () => {
	// Create a source dict with data
	await client.hSet(
		"room:default:config",
		"setting1",
		JSON.stringify("value1"),
	);
	await client.hSet(
		"room:default:config",
		"setting2",
		JSON.stringify("value2"),
	);
	await client.hSet(
		"room:default:config",
		"setting3",
		JSON.stringify("value3"),
	);

	// Direct client call should return 3
	const directLength = await client.hLen("room:default:config");
	expect(directLength).toBe(3);

	// Dict with fallback should also return 3
	const dictWithFallback = new Dict({
		client,
		key: "room:default:config",
		fallback: "room:default:config",
	});

	const fallbackLength = await dictWithFallback.length();
	expect(fallbackLength).toBe(3);
});

test("fallback_dict_with_explicit_policy", async () => {
	// Create a source dict with data
	await client.hSet(
		"room:default:config",
		"setting1",
		JSON.stringify("value1"),
	);
	await client.hSet(
		"room:default:config",
		"setting2",
		JSON.stringify("value2"),
	);
	await client.hSet(
		"room:default:config",
		"setting3",
		JSON.stringify("value3"),
	);

	// Dict with explicit fallback policy "frozen" should work
	const dictWithFrozenPolicy = new Dict({
		client,
		key: "room:default:config",
		fallback: "room:default:config",
		fallbackPolicy: "frozen",
	});

	const frozenLength = await dictWithFrozenPolicy.length();
	expect(frozenLength).toBe(3);
});
