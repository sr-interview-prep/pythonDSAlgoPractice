from data_structures.Sorting.BubbleSort import BubbleSort


def test_bubble_sort():
    bubble_sort = BubbleSort()

    input_list = [2, 4, 5, 1, 3, 6]
    ascending_list = bubble_sort.ascending(input_list=input_list)
    assert ascending_list == sorted(input_list)

    descending_list = bubble_sort.descending(input_list=input_list)
    assert descending_list == sorted(input_list, reverse=True)
