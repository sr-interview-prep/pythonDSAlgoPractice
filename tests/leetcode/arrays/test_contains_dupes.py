from leetcode.arrays.ContainsDupes import ContainsDupes


def test_contains_dupes():
    contains_dupes = ContainsDupes(nums=[2, 5, 3, 5, 2])
    result = contains_dupes.contains_dupes()
    assert result is True

    contains_dupes = ContainsDupes(nums=[2, 5, 3])
    result = contains_dupes.contains_dupes()
    assert result is False
