'''
Given an integer array nums sorted in non-decreasing order, return an array of the squares of each number sorted in non-decreasing order.
Example 1:

Input: nums = [-4,-1,0,3,10]
Output: [0,1,9,16,100]
Explanation: After squaring, the array becomes [16,1,0,9,100].
After sorting, it becomes [0,1,9,16,100].
Example 2:

Input: nums = [-7,-3,2,3,11]
Output: [4,9,9,49,121]
'''
class Solution:
    def sortedSquares(self, nums: List[int]) -> List[int]:
        
        l=0
        r=len(nums)-1
        
        result = []
        while l <= r:
            leftVal  = nums[l] * nums[l]
            rightVal = nums[r] * nums[r]
            if leftVal >= rightVal :
                result.insert(0,leftVal)
                l+=1
            else:
                result.insert(0,rightVal)
                r-=1
                
        return result