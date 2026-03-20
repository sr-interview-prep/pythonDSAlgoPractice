"""
You are given two integer arrays nums1 and nums2, sorted in non-decreasing order, and two integers m and n, representing the number of elements in nums1 and nums2 respectively.

Merge nums1 and nums2 into a single array sorted in non-decreasing order.

The final sorted array should not be returned by the function, but instead be stored inside the array nums1. To accommodate this, nums1 has a length of m + n, where the first m elements denote the elements that should be merged, and the last n elements are set to 0 and should be ignored. nums2 has a length of n.

Example 1:

Input: nums1 = [1,2,3,0,0,0], m = 3, nums2 = [2,5,6], n = 3
Output: [1,2,2,3,5,6]
Explanation: The arrays we are merging are [1,2,3] and [2,5,6].
The result of the merge is [1,2,2,3,5,6] with the underlined elements coming from nums1.

Example 2:

Input: nums1 = [1], m = 1, nums2 = [], n = 0
Output: [1]
Explanation: The arrays we are merging are [1] and [].
The result of the merge is [1].

Example 3:

Input: nums1 = [0], m = 0, nums2 = [1], n = 1
Output: [1]
Explanation: The arrays we are merging are [] and [1].
The result of the merge is [1].
Note that because m = 0, there are no elements in nums1. The 0 is only there to ensure the merge result can fit in nums1.

 """
from typing import List


class MergeTwoSortedArrays(object):
    def __init__(self, nums1: List[int], m: int, nums2: List[int], n: int):
        self.nums1 = nums1
        self.nums2 = nums2
        self.m = m
        self.n = n

    def merge(self):
        i = self.m - 1
        j = self.n - 1
        index1 = self.m + self.n - 1

        while i >= 0 and j >= 0:
            if self.nums1[i] > self.nums2[j]:
                self.nums1[index1] = self.nums1[i]
                i -= 1
                index1 -= 1
            elif self.nums2[j] > self.nums1[i]:
                self.nums1[index1] = self.nums2[j]
                j -= 1
                index1 -= 1
            # When ith and jth elements are equal in value
            else:
                self.nums1[index1] = self.nums1[i]
                self.nums1[index1 - 1] = self.nums2[j]
                i -= 1
                j -= 1
                index1 -= 2
        # If most elements of j are less than elements of i, i will go to zero and we come to this while loop
        while j >= 0:
            self.nums1[index1] = self.nums2[j]
            j -= 1
            index1 -= 1
        return self.nums1
