from data_structures.List.ReverseList import ReverseList


def test_reverse_list_basic():
    reverse_list = ReverseList(input_list=[3, 4, 5, 6])
    result = reverse_list.built_in_method()
    assert result == [6, 5, 4, 3]

    reverse_list = ReverseList(input_list=[3, 4, 5, 6])
    result = reverse_list.linear_method_basic()
    assert result == [6, 5, 4, 3]


def test_reverse_list_inplace():
    reverse_list = ReverseList(input_list=[3, 4, 5, 6])
    result = reverse_list.linear_method_inplace()
    assert result == [6, 5, 4, 3]

    reverse_list = ReverseList(input_list=[3, 4, 5, 6, 7])
    result = reverse_list.linear_method_inplace()
    assert result == [7, 6, 5, 4, 3]
