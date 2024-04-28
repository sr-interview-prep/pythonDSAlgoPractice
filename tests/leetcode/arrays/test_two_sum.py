from leetcode.arrays.TwoSum import TwoSum


def test_two_sum():
    two_sum = TwoSum()
    result = two_sum.execute(nums=[2, 7, 11, 15], target=9)
    assert result == [0, 1]
