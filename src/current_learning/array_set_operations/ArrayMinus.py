"""
Given two integer arrays nums1 and nums2, return an array of elements in nums1 after removing all occurrences of elements found in nums2.
Each occurrence in nums2 removes one occurrence in nums1 (handles duplicates correctly).

Intuition:
- Use a hash map (dictionary) to count occurrences of each number in nums2.
- For each number in nums1, if it is not in nums2's count or its count is 0, keep it; otherwise, decrement the count in nums2 and skip it.
- This ensures each occurrence in nums2 removes only one occurrence in nums1.
- Time: O(n + m), Space: O(min(n, m))
"""
from typing import List

def array_minus(nums1: List[int], nums2: List[int]) -> List[int]:
    hash_map={}
    result=[]
    for num in nums2:
        if num not in hash_map:
            hash_map[num]=1
        else:
            hash_map[num]+=1
    for num in nums1:
        if num not in hash_map:
            result.append(num) 
        else:
            hash_map[num]-=1        
            if hash_map[num]==0:
                hash_map.pop(num)
    return result
            
        
        
        


if __name__ == "__main__":
    # Test cases with assertions
    assert sorted(array_minus([1,2,2,1], [2,2])) == [1,1]
    assert sorted(array_minus([4,9,5], [9,4,9,8,4])) == [5]
    assert sorted(array_minus([1,2,3], [4,5,6])) == [1,2,3]
    assert sorted(array_minus([1,1,1,2], [1,1,2,2])) == [1]
    assert sorted(array_minus([], [1,2,3])) == []
    assert sorted(array_minus([1,2,3], [])) == [1,2,3]
    print("All test cases passed!")
