"""
Given two integer arrays nums1 and nums2, return an array of their intersection.
Each element in the result must appear as many times as it shows in both arrays,
and you may return the result in any order.

Example 1:

Input: nums1 = [1,2,2,1], nums2 = [2,2]
Output: [2,2]
Example 2:

Input: nums1 = [4,9,5], nums2 = [9,4,9,8,4]
Output: [4,9]
Explanation: [9,4] is also accepted."""
from typing import List

'''Solution with sorting'''


class IntersectionOfTwoArrays:
    def __init__(self, nums1: List[int], nums2: List[int]):
        self.nums1 = nums1
        self.nums2 = nums2

    def intersect_method_1(self) -> List[int]:
        i = 0
        j = 0
        output = []
        self.nums1.sort()
        self.nums2.sort()
        while i < len(self.nums1) and j < len(self.nums2):
            if self.nums1[i] < self.nums2[j]:
                i += 1
            elif self.nums2[j] < self.nums1[i]:
                j += 1
            else:
                output.append(self.nums1[i])
                i += 1
                j += 1
        return output

    def intersect_method_2(self) -> List[int]:
        hash_map = {}
        res = []
        for i in self.nums1:
            if i not in hash_map:
                hash_map[i] = 1
            else:
                hash_map[i] += 1
        for j in self.nums2:
            if j in hash_map:
                if hash_map[j] > 0:
                    res.append(j)
                    hash_map[j] -= 1
        return res
