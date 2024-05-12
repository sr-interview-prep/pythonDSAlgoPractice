from data_structures.Search.BinarySearch.BinarySearch import BinarySearch


def test_binary_search():
    binary_search = BinarySearch()
    result = binary_search.execute(nums=[1, 2, 3, 4, 5, 6], target=3)
    assert result == 2
