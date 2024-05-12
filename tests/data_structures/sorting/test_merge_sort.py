from data_structures.Sorting.Merge_Sort.MergeSort import merge_sort


def test_merge_sort():
    input_list = [3, 1, 4, 2]
    sorted_list = merge_sort(my_list=input_list)
    assert sorted_list == sorted(input_list)
