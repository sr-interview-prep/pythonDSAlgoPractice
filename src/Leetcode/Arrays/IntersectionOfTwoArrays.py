'''Given two integer arrays nums1 and nums2, return an array of their intersection. Each element in the result must appear as many times as it shows in both arrays and you may return the result in any order.

 

Example 1:

Input: nums1 = [1,2,2,1], nums2 = [2,2]
Output: [2,2]
Example 2:

Input: nums1 = [4,9,5], nums2 = [9,4,9,8,4]
Output: [4,9]
Explanation: [9,4] is also accepted.'''

'''Solution with sorting'''
class Solution:
    def intersect(self, nums1: List[int], nums2: List[int]) -> List[int]:
        i=0
        j=0
        output=[]
        nums1.sort()
        nums2.sort()
        while i<len(nums1) and j<len(nums2):
            if nums1[i]<nums2[j]:
                i+=1
            elif nums2[j]<nums1[i]:
                j+=1
            else:
                output.append(nums1[i])
                i+=1
                j+=1
        return output

'''HashMap Approach, no need to sort'''

class Solution:
    def intersect(self, nums1: List[int], nums2: List[int]) -> List[int]:
        hashMap={}
        res=[]
        for i in nums1:
            if i not in hashMap:
                hashMap[i]=1
            else:
                hashMap[i]+=1
        for j in nums2:
            if j in hashMap:
                if hashMap[j]>0:
                    res.append(j)
                    hashMap[j]-=1
        return res