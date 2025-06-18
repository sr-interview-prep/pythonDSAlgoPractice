from current_learning.kadanes_algo.max_sub_array import MaxSubArray


def test_max_sub_array():
    max_sub_array = MaxSubArray(nums=[-2, 1, -3, 4, -1, 2, 1, -5, 4])
    result = max_sub_array.get_max_sub_array()
    assert result == 6
