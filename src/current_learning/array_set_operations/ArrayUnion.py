"""
Problem Statement:
Given two integer arrays nums1 and nums2, return an array containing the union of the two arrays (all unique elements from both arrays, no duplicates).
The result can be in any order.

Edge Cases to test:
- Both arrays empty
- One array empty, one non-empty
- Arrays with all elements the same
- Arrays with no overlap
- Arrays with partial overlap
- Arrays with negative numbers and zeros
- Arrays with large numbers
- Arrays with repeated elements
"""
from typing import List

def array_union(nums1: List[int], nums2: List[int]) -> List[int]:
    hash_map={}
    result=[]
    for num in nums1:
        if num not in hash_map:
            hash_map[num]=1
        else:
            hash_map[num]+=1
    for num in nums2:
        result.append(num)
        if num in hash_map:
            hash_map[num]-=1               
            if hash_map[num]==0:
                hash_map.pop(num)
                
    for k,v in hash_map.items():
        for _ in range(v):
            result.append(k)
    print(sorted(result))
    return sorted(result)

if __name__ == "__main__":
    # Test cases for union of lists (with duplicates, but only as many as in the original lists)
    assert sorted(array_union([1,2,2,1], [2,2])) == [1,1,2,2] 
    assert sorted(array_union([4,9,5], [9,4,9,8,4])) == [4,4,5,8,9,9] 
    assert sorted(array_union([1,2,3], [4,5,6])) == [1,2,3,4,5,6]
    assert sorted(array_union([1,1,1], [1,1,1])) == [1,1,10]
    assert sorted(array_union([], [1,2,3])) == [1,2,3]
    assert sorted(array_union([1,2,3], [])) == [1,2,3]
    assert sorted(array_union([], [])) == []
    print("All list-union test cases passed!")





