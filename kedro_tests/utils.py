from collections.abc import MutableMapping
from typing import (
    Any,
    Dict,
    Text,
)


def get_parameters(
    parameters: Dict[Text, Any], parent_key: Text = "params", sep: Text = "."
) -> Dict[Text, Any]:
    """
    Convert dict with parameters into all forms recognized by kedro.
    Refer to `tests/kedro_tests/test_utils.py` for usage example.
    """
    items = []
    for k, v in parameters.items():
        new_key = "params:" + k if parent_key == "params" else parent_key + sep + k
        items.append((new_key, v))
        if isinstance(v, MutableMapping):
            items.extend(get_parameters(v, new_key, sep=sep).items())
    return dict(items)
