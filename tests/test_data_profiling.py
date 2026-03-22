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


def test_column_profile_includes_histogram_for_numeric_column():
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0] * 100)
    profile = column_profile(s)
    assert "histogram" in profile
    assert isinstance(profile["histogram"], list)
    assert len(profile["histogram"]) > 0
    first_bin = profile["histogram"][0]
    assert "bin_start" in first_bin
    assert "bin_end" in first_bin
    assert "count" in first_bin


def test_column_profile_no_histogram_for_non_numeric_column():
    s = pd.Series(["foo", "bar", "baz"] * 100)
    profile = column_profile(s)
    assert "histogram" not in profile


def test_compute_histogram_bins_returns_correct_structure():
    from data_profiling import compute_histogram_bins
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0] * 20)
    bins = compute_histogram_bins(s, n_bins=5)
    assert len(bins) == 5
    assert all("bin_start" in b and "bin_end" in b and "count" in b for b in bins)
    assert sum(b["count"] for b in bins) == 100
