from data_structures.Sorting.BubbleSort import BubbleSort

bubble_sort = BubbleSort()


def test_bubble_sort_ascending():
    # best case
    input_list = [1, 2, 3, 4, 5, 6]
    ascending_list = bubble_sort.sort(input_list=input_list)
    assert ascending_list == sorted(input_list)

    # average case
    input_list = [2, 4, 5, 1, 3, 6]
    ascending_list = bubble_sort.sort(input_list=input_list)
    assert ascending_list == sorted(input_list)
