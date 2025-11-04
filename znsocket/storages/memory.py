import dataclasses
import fnmatch
import threading
import time as time_module
import typing as t
from copy import deepcopy

from sortedcontainers import SortedList

from znsocket.exceptions import DataError, ResponseError


class MemoryStoragePipeline:
    """Pipeline for batching MemoryStorage operations.

    Allows queuing multiple commands to be executed together,
    mimicking Redis pipeline behavior.

    Parameters
    ----------
    storage : MemoryStorage
        The MemoryStorage instance to execute commands on.

    Examples
    --------
    >>> storage = MemoryStorage()
    >>> pipe = storage.pipeline()
    >>> pipe.set("key1", "value1")
    >>> pipe.set("key2", "value2")
    >>> results = pipe.execute()
    """

    def __init__(self, storage: "MemoryStorage") -> None:
        """Initialize the pipeline."""
        self.storage = storage
        self.commands: list[tuple[str, tuple, dict]] = []

    def __enter__(self) -> "MemoryStoragePipeline":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager without executing (matches redis-py behavior)."""
        pass

    def __getattr__(self, name: str) -> t.Callable:
        """Dynamically add methods to the pipeline.

        Any method call is queued and returns self for chaining.
        """
        if name.startswith("_"):
            raise AttributeError(f"No attribute {name}")

        def method(*args, **kwargs):
            """Queue a command and return self for chaining."""
            self.commands.append((name, args, kwargs))
            return self

        return method

    def execute(self) -> list:
        """Execute all queued commands and return results.

        Returns
        -------
        list
            List of results from each command in order.
        """
        results = []
        for cmd_name, args, kwargs in self.commands:
            if not hasattr(self.storage, cmd_name):
                raise AttributeError(f"MemoryStorage has no method '{cmd_name}'")
            result = getattr(self.storage, cmd_name)(*args, **kwargs)
            results.append(result)
        self.commands.clear()
        return results


@dataclasses.dataclass
class MemoryStorage:
    """In-memory storage backend for znsocket server.

    The Storage class provides Redis-compatible data storage operations including
    hash tables, lists, sets, and basic key-value operations. All data is stored
    in memory using Python data structures.

    This implementation is thread-safe using a reentrant lock (RLock) to protect
    all operations on the internal content dictionary. Multiple threads can safely
    access and modify the storage concurrently.

    Parameters
    ----------
    content : dict, optional
        Initial content for the storage. Default is an empty dictionary.

    Attributes
    ----------
    content : dict
        The internal storage dictionary containing all data.
    _lock : threading.RLock
        Reentrant lock for thread-safe access to content (not serialized).

    Examples
    --------
    >>> storage = MemoryStorage()
    >>> storage.hset("users", "user1", "John")
    1
    >>> storage.hget("users", "user1")
    'John'
    """

    content: dict = dataclasses.field(default_factory=dict)
    _lock: threading.RLock = dataclasses.field(
        init=False, repr=False, default_factory=threading.RLock
    )
    _expiry: dict = dataclasses.field(init=False, repr=False, default_factory=dict)

    def _encode_value(self, value):
        """Encode a value to match Redis behavior.

        Redis accepts: bytes, str, int, float (but NOT bool or dict/list/etc)
        """
        if isinstance(value, bytes):
            return value.decode("utf-8")
        elif isinstance(value, bool):
            # Redis specifically rejects bool
            raise DataError(
                "Invalid input of type: 'bool'. Convert to a "
                "bytes, string, int or float first."
            )
        elif isinstance(value, (int, float)):
            # Convert numbers to their string representation
            return str(value)
        elif isinstance(value, str):
            return value
        else:
            # Reject any other type (dict, list, etc.)
            typename = type(value).__name__
            raise DataError(
                f"Invalid input of type: '{typename}'. "
                f"Convert to a bytes, string, int or float first."
            )

    def hset(
        self,
        name: str,
        key: t.Optional[str] = None,
        value: t.Optional[str] = None,
        mapping: t.Optional[dict] = None,
        items: t.Optional[list] = None,
    ):
        """Set field(s) in a hash.

        Parameters
        ----------
        name : str
            The name of the hash.
        key : str, optional
            The field name to set.
        value : str, optional
            The value to set for the field.
        mapping : dict, optional
            A dictionary of field-value pairs to set.
        items : list, optional
            A list of alternating field-value pairs to set.

        Returns
        -------
        int
            The number of fields that were added.

        Raises
        ------
        DataError
            If no key-value pairs are provided or if value is None when key is provided.
        """
        with self._lock:
            if key is None and not mapping and not items:
                raise DataError("'hset' with no key value pairs")
            if value is None and not mapping and not items:
                raise DataError(f"Invalid input of type {type(value)}")
            pieces = []
            if items:
                pieces.extend(items)
            if key is not None:
                pieces.extend((key, value))
            if mapping:
                for pair in mapping.items():
                    pieces.extend(pair)

            if name not in self.content:
                self.content[name] = {}
            for i in range(0, len(pieces), 2):
                # Encode value to match Redis behavior (validates type)
                self.content[name][pieces[i]] = self._encode_value(pieces[i + 1])
            return len(pieces) // 2

    def hget(self, name, key):
        """Get the value of a hash field.

        Parameters
        ----------
        name : str
            The name of the hash.
        key : str
            The field name to get.

        Returns
        -------
        str or None
            The value of the field, or None if the field does not exist.
        """
        with self._lock:
            if self._is_expired(name):
                return None
            try:
                return self.content[name][key]
            except KeyError:
                return None

    def hmget(self, name, keys):
        with self._lock:
            if self._is_expired(name):
                return [None] * len(keys)
            response = []
            for key in keys:
                try:
                    response.append(self.content[name][key])
                except KeyError:
                    response.append(None)
            return response

    def hkeys(self, name):
        with self._lock:
            if self._is_expired(name):
                return []
            try:
                return list(self.content[name].keys())
            except KeyError:
                return []

    def delete(self, name):
        with self._lock:
            try:
                del self.content[name]
                return 1
            except KeyError:
                return 0

    def exists(self, name):
        with self._lock:
            if self._is_expired(name):
                return 0
            return 1 if name in self.content else 0

    def llen(self, name):
        with self._lock:
            if self._is_expired(name):
                return 0
            try:
                return len(self.content[name])
            except KeyError:
                return 0

    def rpush(self, name, value):
        with self._lock:
            try:
                self.content[name].append(value)
            except KeyError:
                self.content[name] = [value]

            return len(self.content[name])

    def lpush(self, name, value):
        with self._lock:
            try:
                self.content[name].insert(0, value)
            except KeyError:
                self.content[name] = [value]

            return len(self.content[name])

    def lindex(self, name, index):
        with self._lock:
            if index is None:
                raise DataError("Invalid input of type None")
            try:
                return self.content[name][index]
            except KeyError:
                return None
            except IndexError:
                return None
            except TypeError:  # index is not an integer
                return None

    def set(
        self,
        name,
        value,
        nx: bool = False,
        xx: bool = False,
        ex: t.Optional[int] = None,
    ):
        """Set the value of a key.

        Parameters
        ----------
        name : str
            The key name.
        value : str, int, float, or bytes
            The value to set.
        nx : bool, optional
            Only set if the key does not exist (default: False).
        xx : bool, optional
            Only set if the key already exists (default: False).
        ex : int, optional
            Expiration time in seconds (default: None).

        Returns
        -------
        bool or None
            True if the set was successful, None if the condition (nx/xx) was not met.

        Raises
        ------
        DataError
            If value or name is None, or if value is of invalid type.
        """
        with self._lock:
            if value is None or name is None:
                raise DataError("Invalid input of type None")

            exists = name in self.content

            # Handle nx (set only if not exists)
            if nx and exists:
                return None

            # Handle xx (set only if exists)
            if xx and not exists:
                return None

            # Encode value to match Redis behavior (validates type)
            self.content[name] = self._encode_value(value)

            # Handle expiration
            if ex is not None:
                self._expiry[name] = time_module.time() + ex

            return True

    def get(self, name, default=None):
        with self._lock:
            if self._is_expired(name):
                return default
            return self.content.get(name, default)

    def incr(self, name: str, amount: int = 1) -> int:
        """Increment the integer value of a key by the given amount.

        Parameters
        ----------
        name : str
            The key name.
        amount : int, optional
            The amount to increment by (default: 1).

        Returns
        -------
        int
            The value after incrementing.

        Raises
        ------
        ResponseError
            If the value is not an integer or cannot be represented as an integer.
        """
        with self._lock:
            if self._is_expired(name):
                # Expired key is treated as non-existent
                self.content[name] = str(amount)
                return amount

            if name not in self.content:
                self.content[name] = str(amount)
                return amount

            try:
                current_value = int(self.content[name])
                new_value = current_value + amount
                self.content[name] = str(new_value)
                return new_value
            except (ValueError, TypeError):
                raise ResponseError("value is not an integer or out of range")

    def decr(self, name: str, amount: int = 1) -> int:
        """Decrement the integer value of a key by the given amount.

        Parameters
        ----------
        name : str
            The key name.
        amount : int, optional
            The amount to decrement by (default: 1).

        Returns
        -------
        int
            The value after decrementing.

        Raises
        ------
        ResponseError
            If the value is not an integer or cannot be represented as an integer.
        """
        return self.incr(name, -amount)

    def smembers(self, name):
        with self._lock:
            try:
                response = self.content[name]
            except KeyError:
                response = set()

            if not isinstance(response, set):
                raise ResponseError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            return response

    def lrange(self, name, start, end):
        with self._lock:
            if end == -1:
                end = None
            elif end >= 0:
                end += 1
            try:
                return self.content[name][start:end]
            except KeyError:
                return []

    def lset(self, name, index, value):
        with self._lock:
            try:
                self.content[name][index] = value
            except KeyError:
                raise ResponseError("no such key")
            except IndexError:
                raise ResponseError("index out of range")

    def lrem(self, name, count, value):
        with self._lock:
            if count is None or value is None or name is None:
                raise DataError("Invalid input of type None")
            if count == 0:
                try:
                    self.content[name] = [x for x in self.content[name] if x != value]
                except KeyError:
                    return 0
            else:
                removed = 0
                while removed < count:
                    try:
                        self.content[name].remove(value)
                        removed += 1
                    except KeyError:
                        return 0

    def sadd(self, name, value):
        with self._lock:
            try:
                self.content[name].add(value)
            except KeyError:
                self.content[name] = {value}

    def flushall(self):
        with self._lock:
            self.content.clear()

    def srem(self, name, value):
        with self._lock:
            try:
                self.content[name].remove(value)
                return 1
            except KeyError:
                return 0

    def smove(self, source, destination, member):
        """Move a member from one set to another atomically.

        Parameters
        ----------
        source : str
            The name of the source set.
        destination : str
            The name of the destination set.
        member : str
            The member to move.

        Returns
        -------
        int
            1 if the element is moved, 0 if the element is not a member of source.

        Raises
        ------
        ResponseError
            If source or destination is not a set.
        """
        with self._lock:
            # Check if source exists and is a set
            if source not in self.content:
                return 0

            if not isinstance(self.content[source], set):
                raise ResponseError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )

            # Check if member exists in source
            if member not in self.content[source]:
                return 0

            # Check if destination exists and is a set (if it exists)
            if destination in self.content and not isinstance(
                self.content[destination], set
            ):
                raise ResponseError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )

            # Remove from source
            self.content[source].remove(member)

            # Add to destination
            if destination not in self.content:
                self.content[destination] = {member}
            else:
                self.content[destination].add(member)

            return 1

    def linsert(self, name, where, pivot, value):
        with self._lock:
            try:
                index = self.content[name].index(pivot)
                if where == "BEFORE":
                    self.content[name].insert(index, value)
                elif where == "AFTER":
                    self.content[name].insert(index + 1, value)
            except KeyError:
                return 0
            except ValueError:
                return -1

    def hexists(self, name, key):
        with self._lock:
            try:
                return 1 if key in self.content[name] else 0
            except KeyError:
                return 0

    def hdel(self, name, key):
        with self._lock:
            try:
                del self.content[name][key]
                return 1
            except KeyError:
                return 0

    def hlen(self, name):
        with self._lock:
            if self._is_expired(name):
                return 0
            try:
                return len(self.content[name])
            except KeyError:
                return 0

    def hvals(self, name):
        with self._lock:
            if self._is_expired(name):
                return []
            try:
                return list(self.content[name].values())
            except KeyError:
                return []

    def lpop(self, name):
        with self._lock:
            try:
                return self.content[name].pop(0)
            except KeyError:
                return None
            except IndexError:
                return None

    def scard(self, name):
        with self._lock:
            if self._is_expired(name):
                return 0
            try:
                return len(self.content[name])
            except KeyError:
                return 0

    def hgetall(self, name):
        """Get all fields and values in a hash.

        Parameters
        ----------
        name : str
            The name of the hash.

        Returns
        -------
        dict
            A copy of the hash dictionary. Returns empty dict if key doesn't exist.

        Raises
        ------
        ResponseError
            If the key exists but is not a hash.
        """
        with self._lock:
            if self._is_expired(name):
                return {}
            if name not in self.content:
                return {}

            value = self.content[name]

            # Check if it's a hash (dict but not a sorted set)
            if isinstance(value, dict):
                # Check if it's a sorted set
                if "sorted" in value and "scores" in value:
                    raise ResponseError(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                    )
                # It's a hash
                return dict(value)
            else:
                # It's some other type (string, list, set)
                raise ResponseError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )

    def copy(self, src, dst):
        with self._lock:
            if self._is_expired(src):
                return False
            if src == dst:
                return False
            if src not in self.content:
                return False
            if dst in self.content:
                return False
            self.content[dst] = deepcopy(self.content[src])
            return True

    def _is_expired(self, name: str) -> bool:
        """Check if a key has expired and clean it up if so.

        Parameters
        ----------
        name : str
            The key name to check.

        Returns
        -------
        bool
            True if the key was expired and removed, False otherwise.
        """
        if name in self._expiry:
            if time_module.time() >= self._expiry[name]:
                # Key has expired, clean it up
                del self._expiry[name]
                if name in self.content:
                    del self.content[name]
                return True
        return False

    def _parse_score_boundary(
        self, value: t.Union[str, float, int]
    ) -> tuple[float, bool]:
        """Parse a score boundary value like '5.0' or '(5.0' for exclusive.

        Parameters
        ----------
        value : str, float, or int
            The boundary value (e.g., '5.0', '(5.0', '-inf', '+inf')

        Returns
        -------
        tuple[float, bool]
            A tuple of (score, is_exclusive)
        """
        if isinstance(value, (int, float)):
            return float(value), False

        value_str = str(value)
        if value_str == "-inf":
            return float("-inf"), False
        if value_str == "+inf":
            return float("inf"), False

        if value_str.startswith("("):
            return float(value_str[1:]), True
        return float(value_str), False

    def zadd(self, name: str, mapping: t.Optional[dict] = None, **kwargs) -> int:
        """Add members with scores to a sorted set.

        Parameters
        ----------
        name : str
            The name of the sorted set.
        mapping : dict, optional
            A dictionary of {member: score} pairs to add.
        **kwargs
            Additional member=score pairs to add.

        Returns
        -------
        int
            The number of new elements added (not including score updates).

        Examples
        --------
        >>> storage = MemoryStorage()
        >>> storage.zadd("leaderboard", {"player1": 100, "player2": 200})
        2
        >>> storage.zadd("leaderboard", player3=150)
        1
        """
        with self._lock:
            if not mapping and not kwargs:
                raise DataError("zadd requires at least one member-score pair")

            # Combine mapping and kwargs
            all_pairs = {}
            if mapping:
                all_pairs.update(mapping)
            all_pairs.update(kwargs)

            # Initialize sorted set if it doesn't exist
            if name not in self.content:
                self.content[name] = {
                    "scores": {},  # member -> score mapping
                    "sorted": SortedList(),  # [(score, member), ...]
                }
            elif (
                not isinstance(self.content[name], dict)
                or "sorted" not in self.content[name]
            ):
                raise ResponseError(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )

            zset = self.content[name]
            added_count = 0

            for member, score in all_pairs.items():
                score = float(score)
                is_new = member not in zset["scores"]

                if is_new:
                    # New member
                    zset["scores"][member] = score
                    zset["sorted"].add((score, member))
                    added_count += 1
                else:
                    # Update existing member's score
                    old_score = zset["scores"][member]
                    if old_score != score:
                        zset["sorted"].discard((old_score, member))
                        zset["scores"][member] = score
                        zset["sorted"].add((score, member))

            return added_count

    def zcard(self, name: str) -> int:
        """Get the number of members in a sorted set.

        Parameters
        ----------
        name : str
            The name of the sorted set.

        Returns
        -------
        int
            The cardinality (number of elements) of the sorted set, or 0 if key doesn't exist.
        """
        with self._lock:
            if self._is_expired(name):
                return 0
            if name not in self.content:
                return 0
            if (
                not isinstance(self.content[name], dict)
                or "sorted" not in self.content[name]
            ):
                return 0
            return len(self.content[name]["sorted"])

    def zrange(self, name: str, start: int, end: int, withscores: bool = False) -> list:
        """Get a range of members from a sorted set by index.

        Parameters
        ----------
        name : str
            The name of the sorted set.
        start : int
            The starting index (0-based, supports negative indices).
        end : int
            The ending index (inclusive, supports negative indices).
        withscores : bool, optional
            If True, return members with their scores.

        Returns
        -------
        list
            List of members, or list of (member, score) tuples if withscores=True.
        """
        with self._lock:
            if self._is_expired(name):
                return []
            if name not in self.content:
                return []
            if (
                not isinstance(self.content[name], dict)
                or "sorted" not in self.content[name]
            ):
                return []

            sorted_list = self.content[name]["sorted"]
            length = len(sorted_list)

            if length == 0:
                return []

            # Handle negative indices
            if start < 0:
                start = max(0, length + start)
            if end < 0:
                end = length + end
            else:
                end = min(end, length - 1)

            if start > end or start >= length:
                return []

            # Extract range
            result = []
            for i in range(start, end + 1):
                if i >= length:
                    break
                score, member = sorted_list[i]
                if withscores:
                    result.append((member, score))
                else:
                    result.append(member)

            return result

    def zrangebyscore(
        self,
        name: str,
        min_score: t.Union[str, float],
        max_score: t.Union[str, float],
        start: t.Optional[int] = None,
        num: t.Optional[int] = None,
        withscores: bool = False,
    ) -> list:
        """Get members in a sorted set within the given score range.

        Parameters
        ----------
        name : str
            The name of the sorted set.
        min_score : str or float
            Minimum score ('-inf', '5.0', or '(5.0' for exclusive).
        max_score : str or float
            Maximum score ('+inf', '10.0', or '(10.0' for exclusive).
        start : int, optional
            Offset to start returning results from.
        num : int, optional
            Maximum number of results to return.
        withscores : bool, optional
            If True, return members with their scores.

        Returns
        -------
        list
            List of members, or list of (member, score) tuples if withscores=True.
        """
        with self._lock:
            # Validate start/num pairing (matching Redis behavior)
            if (start is not None and num is None) or (
                num is not None and start is None
            ):
                raise DataError("start and num must both be specified")

            if self._is_expired(name):
                return []
            if name not in self.content:
                return []
            if (
                not isinstance(self.content[name], dict)
                or "sorted" not in self.content[name]
            ):
                return []

            min_val, min_exclusive = self._parse_score_boundary(min_score)
            max_val, max_exclusive = self._parse_score_boundary(max_score)

            sorted_list = self.content[name]["sorted"]
            result = []

            for score, member in sorted_list:
                # Check min boundary
                if min_exclusive and score <= min_val:
                    continue
                if not min_exclusive and score < min_val:
                    continue

                # Check max boundary
                if max_exclusive and score >= max_val:
                    break
                if not max_exclusive and score > max_val:
                    break

                result.append((member, score))

            # Apply offset and limit
            if start is not None and num is not None:
                result = result[start : start + num]

            # Format output
            if withscores:
                return result
            else:
                return [member for member, score in result]

    def zrevrangebyscore(
        self,
        name: str,
        max_score: t.Union[str, float],
        min_score: t.Union[str, float],
        start: t.Optional[int] = None,
        num: t.Optional[int] = None,
        withscores: bool = False,
    ) -> list:
        """Get members in a sorted set within score range, in reverse order.

        Parameters
        ----------
        name : str
            The name of the sorted set.
        max_score : str or float
            Maximum score ('+inf', '10.0', or '(10.0' for exclusive).
        min_score : str or float
            Minimum score ('-inf', '5.0', or '(5.0' for exclusive).
        start : int, optional
            Offset to start returning results from.
        num : int, optional
            Maximum number of results to return.
        withscores : bool, optional
            If True, return members with their scores.

        Returns
        -------
        list
            List of members in reverse order, or tuples if withscores=True.
        """
        with self._lock:
            # Validate start/num pairing (matching Redis behavior)
            if (start is not None and num is None) or (
                num is not None and start is None
            ):
                raise DataError("start and num must both be specified")

            if self._is_expired(name):
                return []
            if name not in self.content:
                return []
            if (
                not isinstance(self.content[name], dict)
                or "sorted" not in self.content[name]
            ):
                return []

            min_val, min_exclusive = self._parse_score_boundary(min_score)
            max_val, max_exclusive = self._parse_score_boundary(max_score)

            sorted_list = self.content[name]["sorted"]
            result = []

            # Iterate in reverse
            for score, member in reversed(sorted_list):
                # Check max boundary
                if max_exclusive and score >= max_val:
                    continue
                if not max_exclusive and score > max_val:
                    continue

                # Check min boundary
                if min_exclusive and score <= min_val:
                    break
                if not min_exclusive and score < min_val:
                    break

                result.append((member, score))

            # Apply offset and limit
            if start is not None and num is not None:
                result = result[start : start + num]

            # Format output
            if withscores:
                return result
            else:
                return [member for member, score in result]

    def zrem(self, name: str, *values) -> int:
        """Remove one or more members from a sorted set.

        Parameters
        ----------
        name : str
            The name of the sorted set.
        *values
            Members to remove from the sorted set.

        Returns
        -------
        int
            The number of members removed from the sorted set.
        """
        with self._lock:
            if self._is_expired(name):
                return 0
            if name not in self.content:
                return 0
            if (
                not isinstance(self.content[name], dict)
                or "sorted" not in self.content[name]
            ):
                return 0

            zset = self.content[name]
            removed_count = 0

            for member in values:
                if member in zset["scores"]:
                    score = zset["scores"][member]
                    del zset["scores"][member]
                    zset["sorted"].discard((score, member))
                    removed_count += 1

            return removed_count

    def zcount(
        self, name: str, min_score: t.Union[str, float], max_score: t.Union[str, float]
    ) -> int:
        """Count members in a sorted set with scores within the given range.

        Parameters
        ----------
        name : str
            The name of the sorted set.
        min_score : str or float
            Minimum score ('-inf', '5.0', or '(5.0' for exclusive).
        max_score : str or float
            Maximum score ('+inf', '10.0', or '(10.0' for exclusive).

        Returns
        -------
        int
            The number of members within the score range.
        """
        with self._lock:
            if self._is_expired(name):
                return 0
            if name not in self.content:
                return 0
            if (
                not isinstance(self.content[name], dict)
                or "sorted" not in self.content[name]
            ):
                return 0

            min_val, min_exclusive = self._parse_score_boundary(min_score)
            max_val, max_exclusive = self._parse_score_boundary(max_score)

            sorted_list = self.content[name]["sorted"]
            count = 0

            for score, member in sorted_list:
                # Check min boundary
                if min_exclusive and score <= min_val:
                    continue
                if not min_exclusive and score < min_val:
                    continue

                # Check max boundary
                if max_exclusive and score >= max_val:
                    break
                if not max_exclusive and score > max_val:
                    break

                count += 1

            return count

    def setex(self, name: str, time: int, value: str) -> bool:
        """Set the value and expiration of a key.

        Parameters
        ----------
        name : str
            The key name.
        time : int
            Expiration time in seconds.
        value : str
            The value to set.

        Returns
        -------
        bool
            True if successful.
        """
        with self._lock:
            self.set(name, value)
            self._expiry[name] = time_module.time() + time
            return True

    def expire(self, name: str, time: int) -> int:
        """Set a timeout on a key.

        Parameters
        ----------
        name : str
            The key name.
        time : int
            Expiration time in seconds.

        Returns
        -------
        int
            1 if the timeout was set, 0 if key does not exist.
        """
        with self._lock:
            if name not in self.content:
                return 0
            self._expiry[name] = time_module.time() + time
            return 1

    def ttl(self, name: str) -> int:
        """Get the remaining time to live of a key in seconds.

        Parameters
        ----------
        name : str
            The key name.

        Returns
        -------
        int
            - -2 if the key does not exist
            - -1 if the key exists but has no associated expire
            - Remaining time to live in seconds (rounded down) if the key has an expire

        Examples
        --------
        >>> storage = MemoryStorage()
        >>> storage.set("key", "value")
        >>> storage.ttl("key")
        -1
        >>> storage.setex("temp", 10, "value")
        >>> storage.ttl("temp")
        10
        >>> storage.ttl("nonexistent")
        -2
        """
        with self._lock:
            # Check if key exists
            if name not in self.content:
                return -2

            # Check if key is expired (this will clean it up)
            if self._is_expired(name):
                return -2

            # Check if key has an expiration
            if name not in self._expiry:
                return -1

            # Calculate remaining time
            remaining = self._expiry[name] - time_module.time()
            # Return 0 if expired (shouldn't happen due to _is_expired check above)
            # but match Redis behavior which can return 0 briefly before cleanup
            return max(0, int(remaining))

    def scan_iter(
        self, match: t.Optional[str] = None, count: t.Optional[int] = None
    ) -> t.Iterator[str]:
        """Iterate over keys matching a pattern.

        Parameters
        ----------
        match : str, optional
            Glob-style pattern to match keys (e.g., 'user:*', '*:lock:*').
            If None, all keys are returned.
        count : int, optional
            Maximum number of keys to return. If None, all matching keys are returned.
            Note: In Redis, this is a hint for performance, but here it's a hard limit.

        Yields
        ------
        str
            Keys matching the pattern.

        Examples
        --------
        >>> storage = MemoryStorage()
        >>> storage.set("user:1", "Alice")
        >>> storage.set("user:2", "Bob")
        >>> list(storage.scan_iter("user:*"))
        ['user:1', 'user:2']
        >>> list(storage.scan_iter("user:*", count=1))
        ['user:1']
        """
        with self._lock:
            # Create a snapshot of keys to avoid issues with concurrent modification
            keys_snapshot = list(self.content.keys())

        # Iterate over snapshot without holding lock
        yielded = 0
        for key in keys_snapshot:
            # Check expiry (this will acquire lock briefly)
            if not self._is_expired(key):
                if match is None or fnmatch.fnmatch(key, match):
                    yield key
                    yielded += 1
                    if count is not None and yielded >= count:
                        break

    def pipeline(self) -> MemoryStoragePipeline:
        """Create a pipeline for batching operations.

        Returns
        -------
        MemoryStoragePipeline
            A pipeline object for batching multiple commands.

        Examples
        --------
        >>> storage = MemoryStorage()
        >>> pipe = storage.pipeline()
        >>> pipe.set("key1", "val1")
        >>> pipe.set("key2", "val2")
        >>> results = pipe.execute()
        """
        return MemoryStoragePipeline(self)

    def type(self, name: str) -> str:
        """Determine the type of value stored at a key.

        Parameters
        ----------
        name : str
            The key name to check.

        Returns
        -------
        str
            The type of the value stored at the key:
            - "string" for string values
            - "list" for list values
            - "set" for set values
            - "zset" for sorted set values
            - "hash" for hash values
            - "none" if the key does not exist

        Examples
        --------
        >>> storage = MemoryStorage()
        >>> storage.set("key1", "value")
        >>> storage.type("key1")
        'string'
        >>> storage.hset("user", "name", "Alice")
        >>> storage.type("user")
        'hash'
        >>> storage.rpush("mylist", "item")
        >>> storage.type("mylist")
        'list'
        >>> storage.type("nonexistent")
        'none'
        """
        with self._lock:
            # Check if key is expired
            if self._is_expired(name):
                return "none"

            # Check if key exists
            if name not in self.content:
                return "none"

            value = self.content[name]

            # Check for sorted set (dict with "scores" and "sorted" keys)
            if isinstance(value, dict) and "sorted" in value and "scores" in value:
                return "zset"

            # Check for hash (dict without sorted set structure)
            if isinstance(value, dict):
                return "hash"

            # Check for list
            if isinstance(value, list):
                return "list"

            # Check for set
            if isinstance(value, set):
                return "set"

            # Everything else is a string
            return "string"
