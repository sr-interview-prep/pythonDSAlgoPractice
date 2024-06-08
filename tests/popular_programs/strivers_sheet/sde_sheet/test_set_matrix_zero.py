import pytest

from popular_programs.strivers_sde_sheet.sde_sheet.SetMatrixZero.set_matrix_zero import SetMatrixZero


@pytest.fixture
def input_matrix():
    return [[1, 1, 1], [1, 0, 1], [1, 1, 1]]


def test_set_matrix_zero_brute_force(input_matrix):
    set_matrix_zero = SetMatrixZero(matrix=input_matrix)
    brute_force_result = set_matrix_zero.brute_force()
    assert brute_force_result == [[1, 0, 1], [0, 0, 0], [1, 0, 1]]

    set_matrix_zero = SetMatrixZero(matrix=input_matrix)
    better_result = set_matrix_zero.better_approach()
    assert better_result == [[1, 0, 1], [0, 0, 0], [1, 0, 1]]
