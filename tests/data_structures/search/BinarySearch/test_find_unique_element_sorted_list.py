from data_structures.Search.BinarySearch.FindUniqueElementsSortedList.FindUniqueElementSortedList import \
    find_unique_element_sorted_list


def test_find_unique_element_sorted_list():
    sorted_list_beginning = [1, 2, 2]
    unique_element_beginning = find_unique_element_sorted_list(sorted_list_beginning)
    assert unique_element_beginning == 1

    # Test case where unique element is at the beginning of the list
    sorted_list_beginning = [1, 2, 2, 3, 3, 4, 4, 5, 5]
    unique_element_beginning = find_unique_element_sorted_list(sorted_list_beginning)
    assert unique_element_beginning == 1

    # Test case where unique element is at the end of the list
    sorted_list_end = [1, 1, 2, 2, 3, 3, 4, 4, 5]
    unique_element_end = find_unique_element_sorted_list(sorted_list_end)
    assert unique_element_end == 5

    # Test case where unique element is in the middle of the list
    sorted_list_middle = [1, 1, 2, 3, 3, 3, 5, 5, 5]
    unique_element_middle = find_unique_element_sorted_list(sorted_list_middle)
    assert unique_element_middle == 2

    # Test case where unique element is not present
    sorted_list_no_unique = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
    unique_element_none = find_unique_element_sorted_list(sorted_list_no_unique)
    assert unique_element_none is None
