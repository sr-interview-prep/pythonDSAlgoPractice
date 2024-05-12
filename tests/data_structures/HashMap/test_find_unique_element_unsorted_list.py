from data_structures.HashMap.find_unique_element_unsorted_list import find_unique_element_unsorted_list


def test_find_unique_element_unsorted_list():
    input_list = [4, 1, 2, 1, 2]
    assert find_unique_element_unsorted_list(input_list) == 4

    input_list = [2, 2, 1]
    assert find_unique_element_unsorted_list(input_list) == 1

    input_list = [1]
    assert find_unique_element_unsorted_list(input_list) == 1
