from leetcode.arrays.SquaresSortedArray import SquaresSortedArray


def test_squares_sorted_array():
    squares_sorted_array = SquaresSortedArray()
    result = squares_sorted_array.execute(nums=[-4, -1, 0, 3, 10])
    assert result == [0, 1, 9, 16, 100]

    result = squares_sorted_array.execute(nums=[-7, -3, 2, 3, 11])
    assert result == [4, 9, 9, 49, 121]
