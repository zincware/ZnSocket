"""Abstract base class for storage backends."""

import abc
import typing as t


class StorageBackend(abc.ABC):
    """Abstract base class for storage backends.
    
    This class defines the interface that all storage backends must implement
    to be compatible with znsocket. It provides Redis-compatible operations
    for hash tables, lists, sets, and simple key-value storage.
    """

    # Hash operations
    @abc.abstractmethod
    def hset(
        self,
        name: str,
        key: t.Optional[str] = None,
        value: t.Optional[str] = None,
        mapping: t.Optional[dict] = None,
        items: t.Optional[list] = None,
    ) -> int:
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
        """

    @abc.abstractmethod
    def hget(self, name: str, key: str) -> t.Optional[str]:
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

    @abc.abstractmethod
    def hmget(self, name: str, keys: list) -> list:
        """Get multiple hash field values.

        Parameters
        ----------
        name : str
            The name of the hash.
        keys : list
            List of field names to get.

        Returns
        -------
        list
            List of values in the same order as keys, None for missing fields.
        """

    @abc.abstractmethod
    def hkeys(self, name: str) -> list:
        """Get all field names in a hash.

        Parameters
        ----------
        name : str
            The name of the hash.

        Returns
        -------
        list
            List of field names.
        """

    @abc.abstractmethod
    def hvals(self, name: str) -> list:
        """Get all values in a hash.

        Parameters
        ----------
        name : str
            The name of the hash.

        Returns
        -------
        list
            List of values.
        """

    @abc.abstractmethod
    def hgetall(self, name: str) -> dict:
        """Get all fields and values in a hash.

        Parameters
        ----------
        name : str
            The name of the hash.

        Returns
        -------
        dict
            Dictionary of field-value pairs.
        """

    @abc.abstractmethod
    def hexists(self, name: str, key: str) -> int:
        """Check if a hash field exists.

        Parameters
        ----------
        name : str
            The name of the hash.
        key : str
            The field name to check.

        Returns
        -------
        int
            1 if the field exists, 0 otherwise.
        """

    @abc.abstractmethod
    def hdel(self, name: str, key: str) -> int:
        """Delete a hash field.

        Parameters
        ----------
        name : str
            The name of the hash.
        key : str
            The field name to delete.

        Returns
        -------
        int
            1 if the field was deleted, 0 if it didn't exist.
        """

    @abc.abstractmethod
    def hlen(self, name: str) -> int:
        """Get the number of fields in a hash.

        Parameters
        ----------
        name : str
            The name of the hash.

        Returns
        -------
        int
            Number of fields in the hash.
        """

    # List operations
    @abc.abstractmethod
    def llen(self, name: str) -> int:
        """Get the length of a list.

        Parameters
        ----------
        name : str
            The name of the list.

        Returns
        -------
        int
            Length of the list.
        """

    @abc.abstractmethod
    def rpush(self, name: str, value: str) -> int:
        """Push a value to the right end of a list.

        Parameters
        ----------
        name : str
            The name of the list.
        value : str
            The value to push.

        Returns
        -------
        int
            Length of the list after push.
        """

    @abc.abstractmethod
    def lpush(self, name: str, value: str) -> int:
        """Push a value to the left end of a list.

        Parameters
        ----------
        name : str
            The name of the list.
        value : str
            The value to push.

        Returns
        -------
        int
            Length of the list after push.
        """

    @abc.abstractmethod
    def lindex(self, name: str, index: int) -> t.Optional[str]:
        """Get a list element by index.

        Parameters
        ----------
        name : str
            The name of the list.
        index : int
            The index to get.

        Returns
        -------
        str or None
            The value at the index, or None if out of bounds.
        """

    @abc.abstractmethod
    def lrange(self, name: str, start: int, end: int) -> list:
        """Get a range of list elements.

        Parameters
        ----------
        name : str
            The name of the list.
        start : int
            Start index (inclusive).
        end : int
            End index (inclusive, -1 for end of list).

        Returns
        -------
        list
            List of values in the range.
        """

    @abc.abstractmethod
    def lset(self, name: str, index: int, value: str) -> None:
        """Set a list element by index.

        Parameters
        ----------
        name : str
            The name of the list.
        index : int
            The index to set.
        value : str
            The value to set.
        """

    @abc.abstractmethod
    def lrem(self, name: str, count: int, value: str) -> int:
        """Remove elements from a list.

        Parameters
        ----------
        name : str
            The name of the list.
        count : int
            Number of elements to remove (0 for all).
        value : str
            The value to remove.

        Returns
        -------
        int
            Number of elements removed.
        """

    @abc.abstractmethod
    def linsert(self, name: str, where: str, pivot: str, value: str) -> int:
        """Insert an element in a list before or after a pivot value.

        Parameters
        ----------
        name : str
            The name of the list.
        where : str
            "BEFORE" or "AFTER".
        pivot : str
            The pivot value.
        value : str
            The value to insert.

        Returns
        -------
        int
            Length of the list after insertion, or -1 if pivot not found.
        """

    @abc.abstractmethod
    def lpop(self, name: str) -> t.Optional[str]:
        """Remove and return the leftmost element from a list.

        Parameters
        ----------
        name : str
            The name of the list.

        Returns
        -------
        str or None
            The popped value, or None if list is empty.
        """

    # Set operations
    @abc.abstractmethod
    def smembers(self, name: str) -> set:
        """Get all members of a set.

        Parameters
        ----------
        name : str
            The name of the set.

        Returns
        -------
        set
            Set of all members.
        """

    @abc.abstractmethod
    def sadd(self, name: str, value: str) -> None:
        """Add a member to a set.

        Parameters
        ----------
        name : str
            The name of the set.
        value : str
            The value to add.
        """

    @abc.abstractmethod
    def srem(self, name: str, value: str) -> int:
        """Remove a member from a set.

        Parameters
        ----------
        name : str
            The name of the set.
        value : str
            The value to remove.

        Returns
        -------
        int
            1 if the member was removed, 0 if it didn't exist.
        """

    @abc.abstractmethod
    def scard(self, name: str) -> int:
        """Get the cardinality (number of members) of a set.

        Parameters
        ----------
        name : str
            The name of the set.

        Returns
        -------
        int
            Number of members in the set.
        """

    # Key-value operations
    @abc.abstractmethod
    def set(self, name: str, value: str) -> bool:
        """Set a key-value pair.

        Parameters
        ----------
        name : str
            The key name.
        value : str
            The value to set.

        Returns
        -------
        bool
            True if successful.
        """

    @abc.abstractmethod
    def get(self, name: str, default=None) -> t.Any:
        """Get a value by key.

        Parameters
        ----------
        name : str
            The key name.
        default : Any, optional
            Default value if key doesn't exist.

        Returns
        -------
        Any
            The value or default.
        """

    @abc.abstractmethod
    def delete(self, name: str) -> int:
        """Delete a key.

        Parameters
        ----------
        name : str
            The key name.

        Returns
        -------
        int
            1 if the key was deleted, 0 if it didn't exist.
        """

    @abc.abstractmethod
    def exists(self, name: str) -> int:
        """Check if a key exists.

        Parameters
        ----------
        name : str
            The key name.

        Returns
        -------
        int
            1 if the key exists, 0 otherwise.
        """

    @abc.abstractmethod
    def flushall(self) -> None:
        """Clear all data."""

    @abc.abstractmethod
    def copy(self, src: str, dst: str) -> bool:
        """Copy a key to another key.

        Parameters
        ----------
        src : str
            Source key name.
        dst : str
            Destination key name.

        Returns
        -------
        bool
            True if successful, False if source doesn't exist or destination exists.
        """