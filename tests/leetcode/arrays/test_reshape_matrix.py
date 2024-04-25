from leetcode.arrays.ReshapeMatrix import ReshapeMatrix


def test_matrix_reshape():
    matrix_reshape = ReshapeMatrix()
    result = matrix_reshape.matrix_reshape(mat=[[1, 2], [3, 4]], r=1, c=4)
    assert result == [[1, 2, 3, 4]]
    result = matrix_reshape.matrix_reshape(mat=[[1, 2], [3, 4]], r=2, c=2)
    assert result == [[1, 2], [3, 4]]
    result = matrix_reshape.matrix_reshape([[1, 2], [3, 4]], r=2, c=4)
    assert result == [[1, 2], [3, 4]]
