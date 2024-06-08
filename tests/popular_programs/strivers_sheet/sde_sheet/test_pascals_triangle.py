from popular_programs.strivers_sheet.sde_sheet.PascalsTriangle.PascalsTriangle import PascalsTriangle


def test_pascals_variation_1():
    pascals_triangle = PascalsTriangle()
    result = pascals_triangle.get_variation_1(row=5, col=3)
    assert result == 6

    result = pascals_triangle.get_variation_1(row=5, col=4)
    assert result == 4

    result = pascals_triangle.get_variation_1(row=5, col=2)
    assert result == 4

    result = pascals_triangle.get_variation_1(row=3, col=2)
    assert result == 2


def test_pascals_variation_2():
    pascals_triangle = PascalsTriangle()
    result = pascals_triangle.get_variation_2(row_no=2)
    assert result == [1, 1]

    result = pascals_triangle.get_variation_2(row_no=3)
    assert result == [1, 2, 1]

    result = pascals_triangle.get_variation_2(row_no=4)
    assert result == [1, 3, 3, 1]

    result = pascals_triangle.get_variation_2(row_no=5)
    assert result == [1, 4, 6, 4, 1]


def test_pascals_variation_3():
    pascals_triangle = PascalsTriangle()
    result = pascals_triangle.get_variation_3(no_of_rows=2)
    assert result == [[1], [1, 1]]

    result = pascals_triangle.get_variation_3(no_of_rows=3)
    assert result == [[1], [1, 1], [1, 2, 1]]

    result = pascals_triangle.get_variation_3(no_of_rows=4)
    assert result == [[1], [1, 1], [1, 2, 1], [1, 3, 3, 1]]

    result = pascals_triangle.get_variation_3(no_of_rows=5)
    assert result == [[1], [1, 1], [1, 2, 1], [1, 3, 3, 1], [1, 4, 6, 4, 1]]
