import os

import pytest

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "..", "..", "tests", "fixtures")
BASIC_RDB = os.path.join(FIXTURES, "basic.rdb")


@pytest.fixture
def basic_rdb():
    if not os.path.exists(BASIC_RDB):
        pytest.skip(f"fixture not found: {BASIC_RDB}")
    return BASIC_RDB
