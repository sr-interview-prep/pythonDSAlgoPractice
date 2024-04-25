from leetcode.arrays.SearchSorted2DMatrix import SearchSorted2DMatrix


def test_search_sorted_2d_matrix():
    search_sorted_2d_matrix = SearchSorted2DMatrix()
    result = search_sorted_2d_matrix.search_matrix(matrix=[[1, 3, 5, 7], [10, 11, 16, 20], [23, 30, 34, 60]], target=3)
    result is True
    result = search_sorted_2d_matrix.search_matrix_alternate(matrix=[[1, 3, 5, 7], [10, 11, 16, 20], [23, 30, 34, 60]],
                                                             target=3)
    result is True

    result = search_sorted_2d_matrix.search_matrix(matrix=[[1, 3, 5, 7], [10, 11, 16, 20], [23, 30, 34, 60]], target=13)
    result is True

    result = search_sorted_2d_matrix.search_matrix_alternate(matrix=[[1, 3, 5, 7], [10, 11, 16, 20], [23, 30, 34, 60]],
                                                             target=13)
    result is True
