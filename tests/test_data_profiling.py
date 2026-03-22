import pandas as pd
import pytest
from data_profiling import top_values, column_profile


def test_top_values_returns_ten_by_default():
    s = pd.Series(list(range(20)))
    result = top_values(s, n=10)
    assert len(result) == 10


def test_column_profile_top_values_has_ten_entries():
    s = pd.Series(list(range(20)))
    profile = column_profile(s)
    assert len(profile["top_values"]) == 10
