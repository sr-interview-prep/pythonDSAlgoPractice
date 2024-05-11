from data_structures.Sorting.InsertionSort import insertion_sort


def test_insertion_sort():
    input_list = [4, 2, 6, 5, 1, 3]
    sorted_list = insertion_sort(my_list=input_list)
    assert sorted_list == sorted(input_list)
