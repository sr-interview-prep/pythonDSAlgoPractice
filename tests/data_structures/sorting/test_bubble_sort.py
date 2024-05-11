from data_structures.Sorting.BubbleSort import bubble_sort


def test_bubble_sort():
    input_list = [2, 4, 5, 1, 3, 6]
    sorted_list = bubble_sort(my_list=input_list)
    assert sorted_list == sorted(input_list)
