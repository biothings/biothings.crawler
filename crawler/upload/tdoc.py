import warnings
from typing import Union, Iterable, Dict, Callable, Any, Mapping, Container


class TransformDoc(dict):
    """ Document dictionary with easy to use transformation helpers

    """
    def __init__(self, *args, **kwargs):
        super(TransformDoc, self).__init__(*args, **kwargs)
        for key in self.keys():
            if type(key) is not str:
                v = self.pop(key)
                self[str(key)] = v
                warnings.warn(f"Key '{key}' is not of type str, casted to str for use in documents.", RuntimeWarning)
        self._touched_keys = set()

    def __setitem__(self, key, value):
        super(TransformDoc, self).__setitem__(key, value)
        self._touched_keys.add(key)

    def update(self, __m: Mapping[str, Any], **kwargs) -> 'TransformDoc':
        super(TransformDoc, self).update(__m, **kwargs)
        for k in __m.keys():
            self._touched_keys.add(k)
        return self

    def setdefault(self, __key, __default=...):
        self._touched_keys.add(__key)
        return super(TransformDoc, self).setdefault(__key, __default)

    def rename_keys(self, mappings: Mapping[str, str], ignore_key_error: bool = False) -> 'TransformDoc':
        """Rename keys in a document, returning itself.

        Args:
            mappings: Keys are old keys in the document to be renamed, values are the corresponding new names.
            ignore_key_error: Whether this method ignores when old keys does not exist, or raises `KeyError`.
                Defaults to False, will raise `KeyError` when key does not exist.

        Returns:
            Renamed version of the same TransformDoc instance.

        Raises:
            KeyError: When `ignore_key_error` is set to false and attempting to rename a key that does not exist
        """
        for key in mappings.keys():
            try:
                self[mappings[key]] = self.pop(key)
            except KeyError as e:
                if ignore_key_error:
                    pass
                else:
                    raise e
        return self

    def delete_keys(self, keys: Iterable[str], ignore_key_error: bool = True) -> 'TransformDoc':
        """Delete keys in a document, returning itself.

        Args:
            keys: Key names to delete. If only one key, enclose in a list.
            ignore_key_error: Whether this method ignores when keys does not exist, or raises `KeyError`.
                Defaults to True, will NOT raise `KeyError` when key does not exist.

        Returns:
            Same instance of `TransformDoc` with specified keys removed.
        """
        for key in keys:
            try:
                del self[key]
            except KeyError as e:
                if ignore_key_error:
                    pass
                else:
                    raise e
        return self

    def delete_keys_except(self, keep_keys: Container[str]):
        """Remove keys from document except for the keys given.

        :param keep_keys: Keys to keep.
        :return: Updated document.
        """
        delete_keys = [k for k in self.keys() if k not in keep_keys]
        for delete_key in delete_keys:
            del self[delete_key]
        return self

    def delete_unused_keys(self):
        """Remove keys never written or updated after initialization.

        :return: Updated document.
        """
        return self.delete_keys_except(self._touched_keys)

    def transform_keys_values(self, mappings: Mapping[str, Callable[[Any], Mapping[str, Any]]],
                              ignore_key_error: bool = False) -> 'TransformDoc':
        """Update a document using new keys and values, given old keys and functions

        Args:
            mappings: Maps old keys to functions that produces mapping containing new keys and values.
            ignore_key_error: Whether this method ignores when old keys does not exist, or raises `KeyError`.
                Defaults to False, will raise `KeyError` when key does not exist.

        Returns:
            Updated version of the same document.
        """
        for key in mappings:
            try:
                old_value = self.pop(key)
                new_kv = mappings[key](old_value)
                self.update(new_kv)
            except KeyError as e:
                if ignore_key_error:
                    pass
                else:
                    raise e
        return self

    def transform_values(self, mappings: Dict[str, Callable[[Any], Any]],
                         ignore_key_error: bool = False) -> 'TransformDoc':
        """Update a document using new values, replacing old values with the same key

        Args:
            mappings: Maps old keys to functions that produces new values, to replace old values.
            ignore_key_error: Whether this method ignores when old keys does not exist, or raises `KeyError`.
                Defaults to False, will raise `KeyError` when key does not exist.

        Returns:
            Updated version of the same document.
        """
        for key in mappings:
            try:
                self[key] = mappings[key](self[key])
            except KeyError as e:
                if ignore_key_error:
                    pass
                else:
                    raise e
        return self
