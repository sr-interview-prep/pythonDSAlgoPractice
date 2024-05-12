from random import randint

from data_structures.Sorting.Quicksort.QuickSort import quick_sort_helper


def test_quick_sort():
    input_list = [4, 6, 1, 3, 2, 5]
    result = quick_sort_helper(input_list)
    assert result == sorted(input_list)

    input_list = [5, 4, 3, 2, 1]
    result = quick_sort_helper(input_list)
    assert result == sorted(input_list)
    # Test case 3: Reverse sorted list

    input_list2 = [1, 2, 3, 4, 5, 6]
    result2 = quick_sort_helper(input_list2)
    assert result2 == sorted(input_list2)

    input_list3 = [6, 5, 4, 3, 2, 1]
    result3 = quick_sort_helper(input_list3)
    assert result3 == sorted(input_list3)

    # Test case 4: List with duplicates
    input_list4 = [4, 6, 1, 3, 2, 5, 4]
    result4 = quick_sort_helper(input_list4)
    assert result4 == sorted(input_list4)

    # Test case 5: Empty list
    input_list5 = []
    result5 = quick_sort_helper(input_list5)
    assert result5 == sorted(input_list5)

    # Test case 6: List with one element
    input_list6 = [42]
    result6 = quick_sort_helper(input_list6)
    assert result6 == sorted(input_list6)

    # Test case 7: Large list
    input_list7 = [randint(1, 1000) for _ in range(1000)]
    result7 = quick_sort_helper(input_list7)
    assert result7 == sorted(input_list7)
