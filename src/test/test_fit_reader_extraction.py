import types

import pytest

from notebook_llm_lab.ingestion.fit_reader import _extract_fields_from_message


class DummyField:
    def __init__(self, name, value):
        self.name = name
        self.value = value


def test_get_values_dict():
    class Rec:
        def get_values(self):
            return {"speed": 3.5, "distance": 1000}

    rec = Rec()
    out = _extract_fields_from_message(rec)
    assert out["speed"] == 3.5
    assert out["distance"] == 1000


def test_fields_iterable():
    class Rec:
        fields = [DummyField("speed", 2.2), DummyField("distance", 500)]

    rec = Rec()
    out = _extract_fields_from_message(rec)
    assert out["speed"] == 2.2
    assert out["distance"] == 500


def test_fields_dict_of_fieldobjs():
    class Rec:
        fields = {"speed": DummyField("speed", 4.0), "distance": DummyField("distance", 2000)}

    rec = Rec()
    out = _extract_fields_from_message(rec)
    assert out["speed"] == 4.0
    assert out["distance"] == 2000


def test_iterable_fallback():
    class Rec:
        def __iter__(self):
            yield DummyField("speed", 1.5)
            yield DummyField("distance", 250)

    rec = Rec()
    out = _extract_fields_from_message(rec)
    assert out["speed"] == 1.5
    assert out["distance"] == 250


def test_missing_values_return_empty():
    class Rec:
        pass

    rec = Rec()
    out = _extract_fields_from_message(rec)
    assert out == {}
