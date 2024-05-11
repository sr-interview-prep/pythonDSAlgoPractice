from data_structures.Sorting.QuickSort import quick_sort_helper


def test_quick_sort():
    input_list = [4, 6, 1, 7, 3, 2, 5]
    result = quick_sort_helper(input_list)
    assert result == sorted(input_list)
