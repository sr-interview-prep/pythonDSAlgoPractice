from data_structures.Sorting.BubbleSort.BubbleSort import BubbleSort

bubble_sort = BubbleSort()


def test_bubble_sort_ascending():
    # best case
    input_list = [1, 2, 3, 4, 5, 6]
    result = bubble_sort.sort(input_list=input_list)
    assert result[0] == sorted(input_list)
    assert result[1] == len(input_list) - 1

    # average case
    input_list = [2, 4, 5, 1, 3, 6]
    result = bubble_sort.sort(input_list=input_list)
    assert result[0] == sorted(input_list)
    assert result[1] == 15

    # worst case
    input_list = [6, 5, 4, 3, 2, 1]
    result = bubble_sort.sort(input_list=input_list)
    assert result[0] == sorted(input_list)
    assert result[1] == 15
