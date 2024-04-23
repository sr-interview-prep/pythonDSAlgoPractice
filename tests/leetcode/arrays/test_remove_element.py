from leetcode.arrays.RemoveElements import RemoveElementRemainingCount


def test_remove_element_count():
    remove_element = RemoveElementRemainingCount(nums=[3, 3, 3, 2, 2, 2, 3, 3, 4, 5, 3, 3, 5, 5], val=2)
    result = remove_element.remove_element_remaining_count()
    assert result == 7
