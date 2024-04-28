from leetcode.arrays.TwoSumSortedArray import TwoSumSortedArray


def test_two_sum_sorted_array():
    two_sum_sorted_array = TwoSumSortedArray()
    
    result = two_sum_sorted_array.execute(numbers=[2, 3, 4], target=6)
    assert result == [1, 3]

    result = two_sum_sorted_array.execute(numbers=[-1, 0], target=-1)
    assert result == [1, 2]
