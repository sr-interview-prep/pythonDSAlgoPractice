from leetcode.arrays.IntersectionOfTwoArrays import IntersectionOfTwoArrays


def test_intersection_two_arrays():
    intersection_two_arrays = IntersectionOfTwoArrays(nums1=[4, 9, 5], nums2=[9, 4, 9, 8, 4])
    result = intersection_two_arrays.intersect_method_1()
    assert result == [4, 9]

    result = intersection_two_arrays.intersect_method_2()
    assert result == [4, 9]
