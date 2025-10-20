import dataclasses
import typing as t
from copy import deepcopy

from znsocket.exceptions import DataError, ResponseError


@dataclasses.dataclass
class MemoryStorage:
    """In-memory storage backend for znsocket server.

    The Storage class provides Redis-compatible data storage operations including
    hash tables, lists, sets, and basic key-value operations. All data is stored
    in memory using Python data structures.

    Parameters
    ----------
    content : dict, optional
        Initial content for the storage. Default is an empty dictionary.

    Attributes
    ----------
    content : dict
        The internal storage dictionary containing all data.

    Examples
    --------
    >>> storage = Storage()
    >>> storage.hset("users", "user1", "John")
    1
    >>> storage.hget("users", "user1")
    'John'
    """

    content: dict = dataclasses.field(default_factory=dict)

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
            self.content[name][pieces[i]] = pieces[i + 1]
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
        try:
            return self.content[name][key]
        except KeyError:
            return None

    def hmget(self, name, keys):
        response = []
        for key in keys:
            try:
                response.append(self.content[name][key])
            except KeyError:
                response.append(None)
        return response

    def hkeys(self, name):
        try:
            return list(self.content[name].keys())
        except KeyError:
            return []

    def delete(self, name):
        try:
            del self.content[name]
            return 1
        except KeyError:
            return 0

    def exists(self, name):
        return 1 if name in self.content else 0

    def llen(self, name):
        try:
            return len(self.content[name])
        except KeyError:
            return 0

    def rpush(self, name, value):
        try:
            self.content[name].append(value)
        except KeyError:
            self.content[name] = [value]

        return len(self.content[name])

    def lpush(self, name, value):
        try:
            self.content[name].insert(0, value)
        except KeyError:
            self.content[name] = [value]

        return len(self.content[name])

    def lindex(self, name, index):
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

    def set(self, name, value):
        if value is None or name is None:
            raise DataError("Invalid input of type None")
        self.content[name] = value
        return True

    def get(self, name, default=None):
        return self.content.get(name, default)

    def smembers(self, name):
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
        if end == -1:
            end = None
        elif end >= 0:
            end += 1
        try:
            return self.content[name][start:end]
        except KeyError:
            return []

    def lset(self, name, index, value):
        try:
            self.content[name][index] = value
        except KeyError:
            raise ResponseError("no such key")
        except IndexError:
            raise ResponseError("index out of range")

    def lrem(self, name, count, value):
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
        try:
            self.content[name].add(value)
        except KeyError:
            self.content[name] = {value}

    def flushall(self):
        self.content.clear()

    def srem(self, name, value):
        try:
            self.content[name].remove(value)
            return 1
        except KeyError:
            return 0

    def linsert(self, name, where, pivot, value):
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
        try:
            return 1 if key in self.content[name] else 0
        except KeyError:
            return 0

    def hdel(self, name, key):
        try:
            del self.content[name][key]
            return 1
        except KeyError:
            return 0

    def hlen(self, name):
        try:
            return len(self.content[name])
        except KeyError:
            return 0

    def hvals(self, name):
        try:
            return list(self.content[name].values())
        except KeyError:
            return []

    def lpop(self, name):
        try:
            return self.content[name].pop(0)
        except KeyError:
            return None
        except IndexError:
            return None

    def scard(self, name):
        try:
            return len(self.content[name])
        except KeyError:
            return 0

    def hgetall(self, name):
        try:
            return self.content[name]
        except KeyError:
            return {}

    def copy(self, src, dst):
        if src == dst:
            return False
        if src not in self.content:
            return False
        if dst in self.content:
            return False
        self.content[dst] = deepcopy(self.content[src])
        return True
