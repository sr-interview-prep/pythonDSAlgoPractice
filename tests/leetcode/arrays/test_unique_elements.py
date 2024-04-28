from leetcode.arrays.UniqueElements import UniqueElements


def test_unique_elements():
    unique_elements = UniqueElements()
    result = unique_elements.execute(nums=[1, 1, 1, 2, 2, 2, 3, 3, 4, 4])
    assert result == 4
