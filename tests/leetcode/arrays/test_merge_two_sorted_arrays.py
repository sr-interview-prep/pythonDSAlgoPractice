from leetcode.arrays.MergeTwoSortedArray import MergeTwoSortedArrays


def test_merge_two_sorted_arrays():
    merge_two_sorted_arrays = MergeTwoSortedArrays(nums1=[1, 2, 3, 0, 0, 0], m=3, nums2=[2, 5, 6], n=3)
    result = merge_two_sorted_arrays.merge()
    assert result == [1, 2, 2, 3, 5, 6]
