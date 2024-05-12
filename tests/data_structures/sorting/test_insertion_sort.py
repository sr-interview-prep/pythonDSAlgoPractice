from data_structures.Sorting.InsertionSort.InsertionSort import insertion_sort


def test_insertion_sort():
    input_list = [4, 2, 6, 5, 1, 3]
    sorted_list = insertion_sort(my_list=input_list)
    assert sorted_list[0] == sorted(input_list)
    assert sorted_list[1] == 14

    input_list = [1, 2, 3, 4, 5, 6]
    sorted_list = insertion_sort(my_list=input_list)
    assert sorted_list[0] == sorted(input_list)
    assert sorted_list[1] == 5
