# print(Solution.rotateRight([1,2,3,4,5,6,7],3))
# print(Solution.rotateLeft([1, 2, 3, 4, 5, 6, 7], 3))
from leetcode.arrays.RotateArray import RotateArray


def test_rotate_array():
    rotate_array = RotateArray()
    result = rotate_array.rotate_left([1, 2, 3, 4, 5, 6, 7], 3)
    assert result == [4, 5, 6, 7, 1, 2, 3]
    result = rotate_array.rotate_right([1, 2, 3, 4, 5, 6, 7], 3)
    assert result == [5, 6, 7, 1, 2, 3, 4]
