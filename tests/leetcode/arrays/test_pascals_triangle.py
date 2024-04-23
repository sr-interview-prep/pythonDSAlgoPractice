from leetcode.arrays.PascalsTriangle import PascalsTriangle


def test_pascals_triangle():
    pascals_triangle = PascalsTriangle(num_rows=5)
    result = pascals_triangle.generate()
    assert result == [[1], [1, 1], [1, 2, 1], [1, 3, 3, 1], [1, 4, 6, 4, 1]]

    pascals_triangle = PascalsTriangle(num_rows=1)
    result = pascals_triangle.generate()
    assert result == [[1]]
