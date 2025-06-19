"""
Given two integer arrays nums1 and nums2, return an array of their intersection.
Each element in the result must appear as many times as it shows in both arrays,
and you may return the result in any order.

Intuition:
- Use a hash map (dictionary) to count occurrences of each number in nums1.
- For each number in nums2, if it exists in the hash map and the count is positive, add it to the result and decrease the count.
- This ensures each element appears as many times as it appears in both arrays (handles duplicates correctly).
- This approach is optimal: O(n + m) time and O(min(n, m)) space.
"""
from typing import List

def intersect(nums1: List[int], nums2: List[int]) -> List[int]:
    result=[]
    hash_map={}
    for num in nums1:
        if num not in hash_map:
            hash_map[num]=1
        else:
            hash_map[num]+=1
    for num in nums2:
        if num in hash_map:
            if hash_map[num]==0:
                hash_map.pop(num)
            else:
                hash_map[num]-=1
                result.append(num)
    return result
            

if __name__ == "__main__":
    # Test cases with assertions
    assert sorted(intersect([1,2,2,1], [2,2])) == [2,2]
    assert sorted(intersect([4,9,5], [9,4,9,8,4])) == [4,9]
    assert sorted(intersect([1,2,3], [4,5,6])) == []
    assert sorted(intersect([1,1,1,2], [1,1,2,2])) == [1,1,2]
    assert sorted(intersect([], [1,2,3])) == []
    assert sorted(intersect([1,2,3], [])) == []
    print("All test cases passed!")
