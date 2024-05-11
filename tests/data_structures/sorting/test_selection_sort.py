from data_structures.Sorting.SelectionSort import selection_sort


def test_selection_sort():
    input_list = [2, 4, 5, 1, 3, 6]
    sorted_list = selection_sort(input_list)
    assert sorted_list == sorted(input_list)
