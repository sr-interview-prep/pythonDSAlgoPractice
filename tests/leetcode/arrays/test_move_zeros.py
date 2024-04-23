from leetcode.arrays.MoveZeros import MoveZeros


def test_move_zeros():
    move_zeros = MoveZeros(nums=[0, 1, 0, 3, 12])
    result = move_zeros.move_zeros()
    assert result == [1, 3, 12, 0, 0]
