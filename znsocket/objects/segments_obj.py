import json
import typing as t
from collections.abc import MutableSequence

import redis

from znsocket.abc import (
    ZnSocketObject,
)
from znsocket.client import Client

if t.TYPE_CHECKING:
    from znsocket import List


class Segments(ZnSocketObject, MutableSequence):
    """Copy of a list object with piece table segments.

    This class implements a copy-on-write list using segment-based storage for
    efficiency. It's particularly useful for large objects where only small parts
    are modified, avoiding the need to copy the entire object.

    The data is stored in segments represented as tuples:
    (start, end, data), where start and end are the start and end indices of the part
    taken from the data list.

    Parameters
    ----------
    r : Client or redis.Redis
        Connection to the server.
    origin : List
        The original List object to create segments from.
    key : str
        The key in the server to store the segments data.

    Raises
    ------
    TypeError
        If origin is not a List object.

    Examples
    --------
    >>> client = znsocket.Client("http://localhost:5000")
    >>> original_list = znsocket.List(client, "original")
    >>> original_list.extend([1, 2, 3, 4, 5])
    >>> segments = znsocket.Segments(client, original_list, "segments")
    >>> len(segments)
    5
    >>> segments[0]
    1
    """

    def __init__(self, r: Client | t.Any, origin: "List", key: str) -> None:
        from znsocket import List

        if not isinstance(origin, List):
            raise TypeError("origin must be a List")

        self.redis: redis.Redis = r
        self._origin = origin
        self._key = key
        self.converter = origin.converter
        self.convert_nan = origin.convert_nan

        # create a new segments list
        self.redis.lpush(self.key, json.dumps((0, len(origin), origin.key)))

        self._list = List(
            r=self.redis,
            key=key,
            converter=self.converter,
            convert_nan=self.convert_nan,
        )

    @property
    def key(self) -> str:
        """The key in the server to store the data from this segments object.

        Returns
        -------
        str
            The prefixed key used to store this segments object in the server.
        """
        return f"znsocket.Segments:{self._key}"

    def get_raw(self) -> t.Any:
        """Get the raw data of the segments list.

        Returns
        -------
        list
            A list of segments, where each segment is a tuple of (start, end, data).
        """
        # get all segments
        segments = self.redis.lrange(self.key, 0, -1)
        # decode the segments
        return [json.loads(segment) for segment in segments]

    @classmethod
    def from_list(cls, origin: "List", key: str) -> "Segments":
        """Create a Segments object from a list.

        Parameters
        ----------
        origin : List
            The original List object to create segments from.
        key : str
            The key in the server to store the segments data.

        Returns
        -------
        Segments
            A new Segments object based on the original list.

        Raises
        ------
        TypeError
            If origin is not a List object.
        """
        from znsocket import List

        if not isinstance(origin, List):
            raise TypeError("origin must be a List")

        r = origin.redis
        return cls(r, origin, key)

    def __len__(self) -> int:
        """Return the length of the segments list."""
        # get all segments and sum their lengths
        segments = self.redis.lrange(self.key, 0, -1)
        length = 0
        for segment in segments:
            segment = json.loads(segment)
            start, end, _ = segment
            length += end - start
        return length

    def __getitem__(self, index: int) -> t.Any:
        """Get an item from the segments list."""
        from znsocket import List

        single_item = isinstance(index, int)
        if single_item:
            index = [index]
            # TODO: only quqery self.redis.lrange once with the length here as well
        if isinstance(index, slice):
            index = list(range(*index.indices(len(self))))
        # convert any negative indices to positive
        length = len(self)
        index = [i + length if i < 0 else i for i in index]
        # get all segments
        segments = self.redis.lrange(self.key, 0, -1)
        items = []
        size = 0
        for segment in segments:
            segment = json.loads(segment)
            start, end, target = segment
            # TODO: converter and stuff
            lst = List(
                r=self.redis,
                key=target.split(":", 1)[1],
                converter=self.converter,
                convert_nan=self.convert_nan,
            )
            # append to items in range
            for i in index:
                if size <= i < end - start + size:
                    # get the item from the list
                    items.append(lst[i - size + start])
            size += end - start
        if single_item:
            return items[0]
        return items

    def __setitem__(self, index: int, value: t.Any) -> None:
        """Set an item in the segments list."""
        # list to store the values at
        self._list.append(value)
        if index < 0:
            index += len(self)

        # TODO: update the segments, append / insert into the lst
        # [1, 2, 3, 4] -> |insert, x, 2| [1, 2, X, 3, 4]
        # (0, 3, target) -> (0, 2, target), (0, 1, new), (3, 4, target)

        # update the segments
        segments = self.redis.lrange(self.key, 0, -1)
        size = 0
        for idx, segment in enumerate(segments):
            segment = json.loads(segment)
            start, end, target = segment

            if size <= index < end - start + size:
                pos_in_segment = index - size + start
                if target == self._list.key:
                    self._list[pos_in_segment] = value
                else:
                    # we are in the segment
                    self.redis.lset(self.key, idx, "__SETITEM_IDENTIFIER__")
                    if start < pos_in_segment:
                        self.redis.linsert(
                            self.key,
                            "BEFORE",
                            "__SETITEM_IDENTIFIER__",
                            json.dumps((start, pos_in_segment, target)),
                        )
                    self.redis.linsert(
                        self.key,
                        "BEFORE",
                        "__SETITEM_IDENTIFIER__",
                        json.dumps(
                            (len(self._list) - 1, len(self._list), self._list.key)
                        ),
                    )
                    if pos_in_segment + 1 < end:
                        self.redis.linsert(
                            self.key,
                            "BEFORE",
                            "__SETITEM_IDENTIFIER__",
                            json.dumps((pos_in_segment + 1, end, target)),
                        )
                    self.redis.lrem(self.key, 0, "__SETITEM_IDENTIFIER__")
                return

            size += end - start

        raise IndexError("list index out of range")

    def __delitem__(self, index: int) -> None:
        if index < 0:
            index += len(self)

        segments = self.redis.lrange(self.key, 0, -1)
        size = 0
        for idx, segment in enumerate(segments):
            segment = json.loads(segment)
            start, end, target = segment

            if size <= index < end - start + size:
                pos_in_segment = index - size + start
                self.redis.lset(self.key, idx, "__DELITEM_IDENTIFIER__")
                if start < pos_in_segment:
                    self.redis.linsert(
                        self.key,
                        "BEFORE",
                        "__DELITEM_IDENTIFIER__",
                        json.dumps((start, pos_in_segment, target)),
                    )
                if pos_in_segment + 1 < end:
                    self.redis.linsert(
                        self.key,
                        "BEFORE",
                        "__DELITEM_IDENTIFIER__",
                        json.dumps((pos_in_segment + 1, end, target)),
                    )
                self.redis.lrem(self.key, 0, "__DELITEM_IDENTIFIER__")
                return

            size += end - start

        raise IndexError("list index out of range")

    def insert(self, index: int, value: t.Any) -> None:
        """Insert an item in the segments list."""
        if index < 0:
            index += len(self)

        self._list.append(value)

        segments = self.redis.lrange(self.key, 0, -1)
        size = 0
        for idx, segment in enumerate(segments):
            segment = json.loads(segment)
            start, end, target = segment

            if size <= index < end - start + size:
                pos_in_segment = index - size + start
                self.redis.lset(self.key, idx, "__INSERT_IDENTIFIER__")
                if start < pos_in_segment:
                    self.redis.linsert(
                        self.key,
                        "BEFORE",
                        "__INSERT_IDENTIFIER__",
                        json.dumps((start, pos_in_segment, target)),
                    )
                self.redis.linsert(
                    self.key,
                    "BEFORE",
                    "__INSERT_IDENTIFIER__",
                    json.dumps((len(self._list) - 1, len(self._list), self._list.key)),
                )
                if pos_in_segment < end:
                    self.redis.linsert(
                        self.key,
                        "BEFORE",
                        "__INSERT_IDENTIFIER__",
                        json.dumps((pos_in_segment, end, target)),
                    )
                self.redis.lrem(self.key, 0, "__INSERT_IDENTIFIER__")
                return

            size += end - start
        if index == size:
            self.redis.rpush(
                self.key,
                json.dumps((len(self._list) - 1, len(self._list), self._list.key)),
            )
            return
        raise IndexError("list index out of range")
