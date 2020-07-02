from typing import Union, Iterable, Dict, Callable, Any, Mapping


class TransformDoc(dict):
    """ Document dictionary with easy to use transformation helpers

    """
    def __init__(self, *args, **kwargs):
        super(TransformDoc, self).__init__(*args, **kwargs)

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

    def delete_keys(self, keys: Union[str, Iterable[str]], ignore_key_error: bool = True) -> 'TransformDoc':
        """Delete keys in a document, returning itself.

        Args:
            keys: a single key name or list of key names to delete.
            ignore_key_error: Whether this method ignores when keys does not exist, or raises `KeyError`.
                Defaults to True, will NOT raise `KeyError` when key does not exist.

        Returns:
            Same instance of `TransformDoc` with specified keys removed.
        """
        for key in list(keys):
            try:
                del self[key]
            except KeyError as e:
                if ignore_key_error:
                    pass
                else:
                    raise e
        return self

    def update(self, __m: Mapping[str, Any], **kwargs) -> 'TransformDoc':
        super(TransformDoc, self).update(__m, **kwargs)
        return self

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
