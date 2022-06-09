import pytest

from kedro_tests import get_parameters


@pytest.fixture
def parameters():
    return {
        "a": {"a1": 1, "a2": 2},
        "b": {"b1": "text1"},
        "c": {"c1": {"c11": 3, "c12": "text2"}, "c2": {"c21": 4, "c22": "text3"}},
    }


def test_get_parameters(parameters):
    expected = {
        "params:a": {"a1": 1, "a2": 2},
        "params:a.a1": 1,
        "params:a.a2": 2,
        "params:b": {"b1": "text1"},
        "params:b.b1": "text1",
        "params:c": {
            "c1": {"c11": 3, "c12": "text2"},
            "c2": {"c21": 4, "c22": "text3"},
        },
        "params:c.c1": {"c11": 3, "c12": "text2"},
        "params:c.c1.c11": 3,
        "params:c.c1.c12": "text2",
        "params:c.c2": {"c21": 4, "c22": "text3"},
        "params:c.c2.c21": 4,
        "params:c.c2.c22": "text3",
    }
    result = get_parameters(parameters)
    assert expected == result


def test_get_parameters_empty():
    expected = {}
    result = get_parameters({})
    assert expected == result
